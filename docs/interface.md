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

### Common Web Socket Request/Response items

Since we're communicating over web sockets, each request/response is one
javascript object.

| Field | Where | Description |
| --- | --- | --- |
| `rid` | req/res | To tie requests to responses, a `rid` field is used.  The side making the request is responsible for ensuring unique `rid`s. |
| `version` | req | Only for HTTP requsts and on websocket upgrades.  Requests a particular API version.  If no version is specified, the latest is used. |
| `code` | res | Machine-switchable response code. |
| `message` | res | Human readable message.  Only present for non-successful requests |

### Response Codes

| Code | Description |
| --- | --- |
| InternalError | There was an internal error.  Try Again |
| InvalidParameter | You did something wrong.  Fix your request. |

### Read

Reads can either be done with a straight HTTP request or can be made over
a websocket channel.

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

TODO: Should we use the ip address or something else that identifies the client?

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

Response: same as a GET request.

### Register watch

This registers a watch on a list, tied to the client session (cookie).

```
{
    "
    "action": "watch",
    "path": "/data/foo/bar"
}
```

### Unregister watch

```
{
    "action": "unwatch",
    "path": "/data/foo/bar"
}
```

### Receive Notifications

## Node Client

Want a majority of connections?  Always with the leader for writes, others for
notifications when reconfigurations happen.

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
