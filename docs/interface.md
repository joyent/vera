## Interface, v0.0.1

The v0.0.1 interface for Vera is designed for the following use cases:

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

## Perhaps, Perhaps, Perhaps

Things to consider that may end up being bad ideas:

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

Matches the HTTP spec as closely as possble.

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

All writes and notifications are funneled through/from the Vera leader.  That
means there is a specific protocol for finding the leader and establishing a
web socket.

Upon disconnect, a client has some period of time before Vera "times out" the
client's ephemeral data.  Clients must re-establish a connection with the
leader, if possible, before this timeout.  The timeout is configurable by the
client since only the client knows the tolerance for stale data.

If a client attempts to upgrade a connection to a web socket for the Vera
follower, the Vera follower will respond with a `BadRequest` error.

Clients connect by requesting the `/connect` resource, including cookies and
the version header (if possible).  If the leader is unknown to that server,
the follower will return a `ServiceUnavailable` (503) to the client.  If the
leader is elsewhere, the follower will respond with a `TemporaryRedirect` (307).
If it is the leader, it will respond with the appropriate `SwitchingProtocols`
(101).

TODO: Add HTTP request examples for the above.

If the client implementation of the Web Sockets handshake doesn't allow the
client to send headers, the first message that the client sends should be a
message to establish version, etc.  All fields except `rid` and `action` are
optional.

Request (web socket):
```
{
    "rid": "xyz",
    "action": "connect",
    "client": "<client identifier>",
    "version": "0.0.1"
}
```

OK Response.

If the client hasn't established a version with either a header or in the first
message to the server, the server will throw a `BadRequest` response until the
client does so.

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

Reads can either be done with a straight HTTP request or can be made over
a websocket channel.  If the request is made via HTTP, the Vera server will
serve the request, no matter how stale the data may be.  Clients are only
guaranteed consistent reads when requesting over the web soket to the leader.

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
* Need to figure out pagination due to web socket request size.

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

### Register watch

This registers a watch on a list, tied to the client session (cookie).

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
notifications in the order they are recieved.  Clients must ack each
notification they successfully processed.  Vera chooses at least once delivery
(rather than at most once delivery) so clients can receive duplicate
notifications.

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

TODO:
* Still need to work out what data we include in the notification.

Response is in heartbeat.

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
