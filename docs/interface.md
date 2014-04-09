## Interface, v0

The v0 interface for Vera is designed for the following use cases:

1. Data that disappears when a client disconnects ("ephemeral data" in ZK-speak)
2. Notifications to clients when data changes ("watches" in ZK-speak)
3. Leader election

The interface is meant to be as simple as possible to meet the use cases above.
Here is a high-level description of the interface:

1. All paths are to a list of objects.
2. All objects are ephemeral (disappear when the client that is "holding them"
   goes away).
3. Clients can choose to push and watch or only watch.
4. Clients can read (potentially stale) state from any Vera peer.

## API

The interface to Vera is defined using traditional web-based protocols.
Interactions from a browser to Vera should not only be possible, but *simple*.
To that end:

* Client sessions are managed via cookies.
* Clients connect to and communicate with Vera via Web Sockets.
* Intra-Vera communication is secured via HTTPS and HTTP Signature Auth.

### Perhaps, Perhaps, Perhaps

Things to consider that may end up being not implemented or just bad ideas:

* Directories: Make the difference between a directory and a list explicit,
  don't allow name collisions.
* There are no explicit deletes.  Data is deleted when a client disconnects.
* etags for test and set.
* Persistent data.  Perhaps "owner" is "nobody"?
* Assign data ownership to another client.
* Should we establish a majority of connections?  Always one with the leader for
  writes, others for notifications when reconfigurations happen?

### Common Web Socket Request/Response items

Since we're communicating over web sockets, each request/response is one
javascript object.

| Field | Where | Description |
| --- | --- | --- |
| `rid` | req/res | To tie requests to responses, a `rid` field is used.  The side making the request is responsible for ensuring unique `rid`s. |
| `version` | req | Only for HTTP requsts and on websocket upgrades.  Requests a particular API version.  If no version is specified, the latest is used. |
| `code` | res | Machine-switchable response code. |
| `message` | res | Human readable message.  Only present for non-successful requests |
| `commitIndex` | res | Only interesting for follower reads, is the commit index for the follower that served the request. |

### Response Codes and Common Responses

Matches the HTTP spec as closely as possble.  The HTTP codes are only for
reference, the codes do not appear in web socket responses.

| Code | HTTP Code | Description |
| --- | --- | --- |
| SwitchingProtocols | 101 | Web Socket upgrade. |
| OK | 200 | Everything worked as expected |
| TemporaryRedirect | 307 | The leader is elsewhere... |
| BadRequest | 400 | You did something wrong.  Fix your request. |
| NotFound | 404 | Data wasn't found at the requested path. |
| InternalServerError | 500 | There was an internal error.  Try again later. |
| ServiceUnavailable | 503 | The service is unavailable.  Most likely there's a leader election in progress.  Try again later. |

OK Response:
```
{
   "rid": "xyz",
   "code": "OK"
}
```

### Vera Paths

| Path Prefix | Description |
| --- | --- |
| `/connect` | Attempts to connect a web socket. |
| `/data` | Data lives here. |
| `/vera` | Internal cluster management endpoint. |

### Establishing a Web Socket

All writes/notifications are funneled to/originate from the Vera leader.  The
implication is that clients must be able to find and establish a connection with
the current Vera leader.  This section describes how that is done.

Clients connect by requesting the `/connect` resource, including cookies and
the version header (if possible).  If the leader is unknown to that server,
the follower will return a `ServiceUnavailable` (503) to the client.  If the
leader is elsewhere, the follower will respond with a `TemporaryRedirect` (307).
If it is the leader, it will respond with the appropriate `SwitchingProtocols`
(101).

Client connect request:
```
GET /connect HTTP/1.1
Host: vera.example.com
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Origin: http://example.com
Sec-Websocket-Protocol: vera-0
Sec-WebSocket-Version: 13
Cookie: CID=<client identifier>

```

_Note: The CID cookie is only sent for clients that have a previous session._

Server response when leader is unknown:
```
HTTP/1.1 503 Service Unavailable
Retry-After: 5

```

Server response to redirect:
```
HTTP/1.1 307 Temporary Redirect
Location: 0af36c0b.vera.example.com

```

Server response to upgrade:
```
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-Websocket-Protocol: vera-0
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
Set-Cookie: CID=<id>; Domain=vera.example.com; Path=/; Expires=...

```

If the client implementation of the Web Sockets handshake doesn't allow the
client to send headers, the first message that the client sends should be a
message to establish version, etc.  All fields except `rid` and `action` are
optional.

Request (web socket):
```
{
    "rid": "xyz",
    "action": "connect",
    "cid": "<client identifier>",
    "version": "0",
    "timeout": "3600"
}
```

OK Response.

If the client hasn't established a version with either a header or in the first
message to the server, the server will throw a `BadRequest` response until the
client does so.

Upon disconnect, a client has some period of time before Vera "times out" the
client's ephemeral data.  Clients must re-establish a connection with the
leader, before this timeout.  The timeout is configurable by the client since
only the client knows the tolerance for stale data.

Gotchas:

Using cookies and connecting to a different host isn't going to work in a web
browser without a very specific setup, namely, all servers in the cluster are
in an A record for a name, then each individual vera server has a CNAME (or an
A record) at a subdomain.  For example:
```
$ dig +short vera.example.com
10.99.99.19
10.99.99.12
10.99.99.21
$ dig +short 0af36c0b.vera.example.com
10.99.99.19
...
```

### Heartbeat

Clients heartbeat the server to avoid ephemeral data timeouts.  They heartbeat
with the latest known (or unknown) notification index they have processed.  This
serves as both a keep alive for the web socket as well as the mechanism that
a client uses to ack notifications (see later).  If the client has never
received notifications, the index is optional.

Request:
```
{
    "action": "hearbeat",
    "index": 1724
}
```

OK Response.

### Read

Reads can either be done with a straight HTTP request or can be made over a
websocket channel.  If the request is made via HTTP, the Vera server will serve
the request, no matter how stale the data may be.  Clients are only guaranteed
consistent reads when requesting over a web socket (only to a leader).

Request (web socket):
```
{
   "rid": "xyz",
   "action": "get",
   "path": "/data/foo/bar"
}
```

Request (HTTP):
```
GET /data/foo/bar HTTP/1.1
host: vera.us-east.joyent.us
...


```

Response:
```
{
    "rid": "xyz",
    "items": [
        {
            "version": 1233,
            "owner": 10.99.99.14,
            "data": { ... }
        }, {
            "version": 1247,
            "owner": 10.99.99.17,
            "data": { ... }
        }
    ]
}
```

The version is the RAFT index.  The owner is the ip address of the client
holding the watch.

TODO
* Should we use the ip address or something else that identifies the client?

### Push

This pushes an object onto a list.  Pushes must be done over a web socket.
Any data created by a client will be removed when the client is no longer
connected to Vera.

Request:
```
{
    "rid": "xyz",
    "action": "push",
    "path": "/data/foo/bar",
    "data": { ... }
}
```

Response: Same as response to a GET request.

TODO:
* Add an etag example?  Only if we do test/set.

### Register watch

This registers a watch on a list, tied to the client session.

```
{
    "rid": "xyz",
    "action": "watch",
    "path": "/data/foo/bar"
}
```

OK Response.

### Unregister watch

```
{
    "rid": "xyz",
    "action": "unwatch",
    "path": "/data/foo/bar"
}
```

OK Response.

### Receive Notifications

Notifications are sent from the server to the client.  Clients must process
notifications in the order they are recieved.  Clients ack notification they
successfully processed in a heartbeat request.  Vera chooses at least once
delivery (rather than at most once delivery) so clients can receive duplicate
notifications.  Since all notifications are tied to a Raft index, clients can
disregard notifications that have a lower index than what they are heartbeating
with.

Request (from server):
```
{
    "action": "notify",
    "notifications": [
        {
            "index": 1233,
            "data": { ... }
        }, {
            "index": 1247,
            "data": { ... }
        }
    ]
}
```

The index is the raft index corresponding to the change.

Responses to notifications are communicated via heartbeat requests.  Immediate
response is optional since clients should send heartbeats at regular intervals.
If the "next interval" is "a long time in the future", clients should ack with a
heartbeat sooner.  Clients should error on the side of more heartbeats.

TODO:
* Still need to work out what data we include in the notification.

## Node Client

TODO

## Notes/Thoughts

Goal is for notifications to be sent as soon as possible after a client is
disconnected.  There are two problems here:

1. How to determine when a client is disconnected.
2. An efficient way to get clients notified as soon as possible.

If we go with a 1-1 connection with a client, that 1-1 connection could easily
be over a bad channel.  We'd need some way for the client to reconnect to a
different host.

The easiest thing to do here is allow all reads to be allowed from any client.
Then all epemeral writes need to be done via the leader.  When the client fails
the leader detects and rearranges data.  When the leader fails, the clients have
some grace period to connect to the new leader, where all their state is already
in the state machine of the followers.
