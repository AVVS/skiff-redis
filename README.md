# skiff-redis

[![Build Status](https://travis-ci.org/AVVS/skiff-redis.svg?branch=master)](https://travis-ci.org/AVVS/skiff-redis)
[![Dependency Status](https://david-dm.org/AVVS/skiff-redis.svg)](https://david-dm.org/AVVS/skiff-redis)

Implementation of redis-backed skiff persistence layer.
Original abstract class is located here: https://github.com/pgte/abstract-skiff-transport

Library to be used with is here: https://github.com/pgte/skiff

This can not be extended with level-* plugins as it relies on a different store.
If you want to use it with redisDOWN - look at the skiff-level with appropriate backend.

# Install

```bash
$ npm install skiff-redis -S
```

# Create

```javascript
var Skiff = require('skiff-redis');
var Redis = require('ioredis');

var options = {
  redis: new Redis(),
  namespace: '{skiff-redis}' // used as prefix for redis keys.
};

var node = new Skiff('tcp+msgpack://localhost:8081', options);
```

## The URL

The URL contains:

* The protocol: for now we only support `tcp+msgpack`.
* The host name to bind to
* The port to listen to for other peers

If `options.autoListen` is true (the default), the node will listen for peer connections on this URL.


## Options

* `autoListen`: start listening on bootup. defaults to `true`.
* `transport`: transport type. Supported values:
  * `tcp-msgpack`: Msgpack over TCP (default)
  * any module respecting the [transport interface](https://github.com/pgte/abstract-skiff-transport).
* `redis`: ioredis client instance or connection options in the Array format `[port]`, `[port, host]`, `[port, host, options]`
* `waitLeaderTimeout`: the time to wait to become a leader. defaults to 3000 (ms).
* `standby`: if true, will start at the `standby` state instead of the `follower` state. In the `standby` state the node only waits for a leader to send commands. Defaults to `false`.
* `cluster`: the id of the cluster this node will be a part of
* `uuid`: function that generates a UUID. Defaults to using the [`cuid`](https://github.com/ericelliott/cuid) package.
* `heartbeatInterval`: the interval between heartbeats sent from leader. defaults to 50 ms.
* `minElectionTimeout`: the minimum election timeout. defaults to 150 ms.
* `maxElectionTimeout`: the maximum election timeout. defaults to 300 ms.
* `commandTimeout`: the maximum amount of time you're willing to wait for a command to propagate. Defaults to 3 seconds. You can override this in each command call.
* `retainedLogEntries`: the maximum number of log entries that are committed to the state machine that should remain in memory. Defaults to 50.
* `metadata`: to be used by plugins if necessary

# Use

## .listen(cb)

Listen for nodes connecting to us. (if `options.autoListen` is true, your node is already listening).

## .join(url, cb)

Joins a node given its URL.

## .leave(url, cb)

Leaves a node given its URL.

## .peerMeta(url)

Return the peer metadata (if the node knows about such peer).

# Setting up a cluster

To boot a cluster, start a node and wait for it to become a leader. Then, create each additional node in the `standby` mode (`options.standby: true`) and do `leader.join(nodeURL)`.

See [this test](https://github.com/pgte/skiff/blob/master/tests/networking.js#L27) for an actual implementation.

# Events

A Skiff node emits a bunch of events that may or not interest you. In no particular order:

* `error (error)` - if something goes terribly bad.
* `warning (warning)` - if something goes mildly bad.
* `loaded` - once the node persisted state is loaded on bootup
* `applied log (logIndex)` - after a certain log index is applied to the state machine.
* `election timeout` - when a node detects a timeout and will become a candidate because of that.
* `joined (peer)` - when a node joins a given peer.
* `connected(peer)` - when a node is connected to a given peer.
* `disconnected(peer)` - when a node is disconnected from a peer.
* `connecting(peer)` - when a node is trying to connect to a peer.
* `reconnected(peer)` - when a node is reconnected to a peer.
* `outgoing call (peer, type, args)` - when a node sends an RPC call to a peer.
* `response (peer, err, args)` - when a node receives a response from a peer.
* `left (peer)` - when a node leaves a peer
* `state (state)` - after a node has transitioned to a given state
* `leader (node)` - after a node has transitioned to the leader state
* `candidate (node)` - after a node has transitioned to the candidate state
* `follower (node)` - after a node has transitioned to the follower state
* `stopped (node)` - after a node has transitioned to the stopped state
* `standby (node)` - after a node has transitioned to the standby state
* `RequestVote (args)` - when a node receives a RequestVote RPC call
* `AppendEntries (args)` - when a node receives an AppendEntries RPC call
* `InstallSnapshot (args)` - when a node receives an InstallSnapshot RPC call
* `reply (args)` - once a node has replied to an RPC call

## License

MIT
