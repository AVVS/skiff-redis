'use strict';

import msgpack5 from 'msgpack5';
import SkiffPersistence from 'abstract-skiff-persistence';
import _ from 'lodash';
import async from 'neo-async';
import { Writable } from 'readable-stream';
import From from 'from';

let msgpack = msgpack5();

function encode(val) {
  return msgpack.encode(val).slice();
}

function decode(buf) {
  return msgpack.decode(buf);
}

class SkiffRedisWritable extends Writable {

    constructor(stateKey, redis) {
        super({ objectMode: true });
        this._key = stateKey;
        this._redis = redis;
        this._redis.on('error', this._onError);
        this.on('finish', this.destroy);
    }

    _onError(err) {
        this.emit('error', err);
        this.destroy();
    }

    _write(operation, enc, next) {
        switch (operation.type) {
            case 'del':
                this._redis.hdel(this._key, operation.key, next);
                break;

            case 'put':
                this._redis.hset(this._key, operation.key, encode(operation.value), next);
                break;
        }
    }

    destroy() {
        this.removeListener('finish', this.destroy);
        this._redis.removeListener('error', this._onError);
        this._redis = null;
        this._key = null;
    }

}

class SkiffRedis extends SkiffPersistence {

    constructor(options={}) {

        super(options);

        let { redis, namespace } = options;

        // in case we use redis cluster, make sure that namespace is hashed
        this._namespace = namespace || '{skiff-redis}';

        this._redis = redis;

        this._redis.on('error', this._onError);

        this.nodes = {};
    }

    _onError(err) {
        this.emit('error', err);
    }

    _key(...parts) {
        if (parts.length === 0) {
            throw new Error('parts must include at least one value');
        }

        return this._namespace + parts.join('~');
    }

    _saveMeta(nodeId, state, callback) {
        let redis = this._redis;
        let pipeline = redis.pipeline();
        let log = state.log;
        let logEntries = log.entries;

        // do not enter hashtable mode
        state.log = _.omit(log, ['entries']);

        pipeline.set(this._key(nodeId, 'meta'), encode(state));

        let minLogIndex = logEntries.length && logEntries[0].index || 0;
        let maxLogIndex = logEntries.length && logEntries[logEntries.length - 1].index || Infinity;
        let logNamespace = this._key(nodeId, 'logs');
        let cursor = 0;
        let maxReadIndex = 0;

        async.doUntil(
            function zscanRedis(next) {
                redis.zscanBuffer(logNamespace, cursor, 'count', 10, function zscanResponse(err, response) {
                    if (err) {
                        return next(err);
                    }

                    // update cursor
                    cursor = parseInt(response[0], 10);

                    // process log entries
                    let entries = response[1];
                    for (let i = 0, l = entries.length; i < l; i += 2) {
                        let index = parseInt(entries[i + 1], 10);
                        let entry = entries[i];

                        if (index > maxReadIndex) {
                            maxReadIndex = index;
                        }

                        if (index < minLogIndex || index > maxLogIndex) {
                            pipeline.zrem(logNamespace, entry);
                        } else {
                            let correspondingNewEntry = logEntries[index - minLogIndex];
                            let decodedEntry = decode(entry);

                            if (correspondingNewEntry.uuid !== decodedEntry.uuid) {
                                pipeline.zrem(logNamespace, entry);
                                pipeline.zadd(logNamespace, correspondingNewEntry.index, encode(correspondingNewEntry));
                            }
                        }

                    }

                    next();

                });
            },

            function isIterationComplete() {
                return cursor === 0;
            },

            function saveMeta(err) {
                if (err) {
                    return callback(err);
                }

                if (maxReadIndex < maxLogIndex) {
                    logEntries.slice(maxReadIndex - minLogIndex + 1).forEach(function (entry) {
                        pipeline.zadd(logNamespace, entry.index, encode(entry));
                    });
                }

                pipeline.exec(callback);
            }

        );
    }

    _loadMeta(nodeId, callback) {

        this._redis
            .pipeline()
            .getBuffer(this._key(nodeId, 'meta'))
            .zrangebyscoreBuffer(this._key(nodeId, 'logs'), '-inf', '+inf')
            .exec(function (___, results) {
                // err is always null

                let [err, meta] = results[0];
                let [logerr, logEntries] = results[1];

                if (err || logerr) {
                    return callback(err || logerr);
                }

                if (meta) {
                    meta = decode(meta);
                    meta.log.entries = logEntries.map(decode);
                }

                callback(null, meta);

            });

    }

    _applyCommand(nodeId, commitIndex, command, callback) {
        let stateKey = this._key(nodeId, 'state');
        let commitKey = this._key(nodeId, 'commitIndex');
        let pipeline = this._redis.pipeline();

        function mapCommand(op) {
            switch (op.type) {
                case 'put':
                    pipeline.hset(stateKey, op.key, encode(op.value));
                    break;
                case 'del':
                    pipeline.hdel(stateKey, op.key);
                    break;
            }
        }

        if (command.type === 'batch' && command.operations) {
            command.operations.forEach(mapCommand);
        } else {
            mapCommand(command);
        }

        pipeline.set(commitKey, commitIndex);

        pipeline.exec(callback);
    }

    _lastAppliedCommitIndex(nodeId, callback) {
        this._redis.get(this._key(nodeId, 'commitIndex')).then(Number).nodeify(callback);
    }

    _saveCommitIndex(nodeId, commitIndex, callback) {
        this._redis.set(this._key(nodeId, 'commitIndex'), commitIndex, callback);
    }

    _createReadStream(nodeId) {
        let stateKey = this._key(nodeId, 'state');
        let redis = this._redis;
        let cursor = 0;
        let readable = From(function getChunk(count, next) {

            redis.hscanBuffer(stateKey, cursor, 'count', count || 10, (err, response) => {
                if (err) {
                    this.emit('error', err);
                    this.destroy();
                    return next();
                }

                cursor = parseInt(response[0], 10);

                // hash
                let sets = response[1];

                for (let i = 0, l = sets.length; i < l; i += 2) {
                    let hashKey = sets[i];
                    let hashValue = decode(sets[i + 1]);

                    this.emit('data', { key: hashKey.toString(), value: hashValue });
                }

                if (cursor === 0) {
                    this.emit('end');
                }

                next();

            });

        });

        return readable;
    }

    _createWriteStream(nodeId) {
        return new SkiffRedisWritable(this._key(nodeId, 'state'), this._redis);
    }

    _removeAllState(nodeId, callback) {
        this._redis.del(this._key(nodeId, 'state'), callback);
    }

    _close(callback) {
        this._redis.disconnect();
        this._redis.removeListener('error', this._onError);
        callback();
    }

}


export default SkiffRedis;
