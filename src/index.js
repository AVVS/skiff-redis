'use strict';

import SkiffNode from 'skiff-algorithm';
import SkiffRedis from './persistence.js';

// options
import defaultOptions from './default_options';
import { EventEmitter } from 'events';
import _ from 'lodash';
import propagate from 'propagate';
import async from 'neo-async';
import Redis from 'ioredis';
import * as transports from './transport.js';

let beforeLeaderOps = ['listen', 'waitLeader', 'close'];

class SkiffClient extends EventEmitter {

    constructor(id, options={}) {
        super();

        if (!id) {
            throw new Error('node id must be defined');
        }

        _.bindAll(this, [ '_listening', '_work' ]);

        this.id = id;

        this._options = _.extend({}, defaultOptions, options);

        let redisOpts = this._options.redisClient;

        if (!(redisOpts instanceof Redis)) {
            if (Array.isArray(redisOpts)) {
                switch (redisOpts.length) {
                    case 0:
                    case 1:
                        this._options.redisClient = new Redis(redisOpts[0]);
                        break;
                    case 2:
                        this._options.redisClient = new Redis(redisOpts[0], redisOpts[1]);
                        break;
                    default:
                        this._options.redisClient = new Redis(redisOpts[0], redisOpts[1], redisOpts[2]);
                        break;
                }
            } else {
                this._options.redisClient = new Redis(redisOpts);
            }
        }

        this._db = new SkiffRedis({ namespace: this._options.namespace, redis: this._options.redisClient });
        this._options.id = id;
        this._options.persistence = this._db;
        this._options.transport = new (transports.resolve(this._options.transport))();
        this.node = new SkiffNode(this._options);

        propagate(this.node, this);

        this.node.on('error', (err) => {
            this.emit('error', err);
        });

        // propagate certain persistence layer events
        this._options.persistence.on('error', (err) => {
            this.emit('error', err);
        });

        // propagate certain transport layer events
        this._options.transport.on('error', (err) => {
            this.emit('error', err);
        });

        // queue
        this.queue = async.queue(this._work, 1);

        if (this._options.autoListen) {
            this.queue.push({ op: 'listen', args: [ this._listening ] });
        }

        this.queue.push({ op: 'waitLeader' });
    }

    // Public API

    put(key, value, ...opts) {
        let callback = opts.pop();
        let options = opts.pop();

        this.queue.push({
            op: 'put',
            args: [ key, value, options, callback ]
        });
    }

    del(key, ...opts) {
        let callback = opts.pop();
        let options = opts.pop();

        this.queue.push({
            op: 'del',
            args: [ key, options, callback ]
        });
    }

    batch(ops, ...opts) {
        let callback = opts.pop();
        let options = opts.pop();

        this.queue.push({
            op: 'batch',
            args: [ ops, options, callback ]
        });
    }

    get(key, ...opts) {
        let callback = opts.pop();
        let options = opts.pop();

        this.queue.push({
            op: 'get',
            args: [ key, options, callback ]
        });
    }

    iterator(options) {

    }

    createReadStream(options) {
        return this._db._createReadStream(this.node.id, options);
    }

    createWriteStream(options) {
        return this._db._createWriteStream(this.node.id, options);
    }

    join(node, ...opts) {
        let callback = opts.pop();
        let metadata = opts.pop() || null;

        this.node.join(node, metadata, callback);
    }

    leave(node, callback) {
        this.node.leave(node, callback);
    }

    peerMeta(node) {
        this.node.peerMeta(node);
    }

    listen(callback) {
        this.node.listen(this.node.id, (err) => {
            if (err) {
                if (typeof callback === 'function') {
                    callback(err);
                } else {
                    this.emit('error', err);
                }
                return;
            }

            if (typeof callback === 'function') {
                callback();
            }
        });
    }

    close(callback) {
        this.queue.push({
            op: 'close',
            args: [callback]
        });
    }

    open(callback) {
        this._waitLeader(callback);
    }

    // Private API

    _listening(err) {
        if (err) {
            this.emit('error', err);
        }
    }

    _work(item, next) {
        let method = '_' + item.op;
        let args = item.args || [];
        let appCallback = args[args.length - 1];
        let typeofAppCallback = typeof appCallback;

        if (typeofAppCallback === 'function' || typeofAppCallback === 'undefined') {
            args.pop();
        } else {
            appCallback = undefined;
        }

        let callback = function () {
            if (appCallback) {
                appCallback.apply(null, arguments);
            }

            setImmediate(next);
        };


        if (beforeLeaderOps.indexOf(item.op) == -1 && this.node.state.name !== 'leader') {
            let err = new Error(item.op + ' operation requires being the leader');
            err.leader = this.node.currentLeader();
            return callback(err);
        }

        args.push(callback);
        this[method].apply(this, args);

    }

    _listen(callback) {
        this.listen(callback);
    }

    _waitLeader(next) {
        next = _.once(next);

        if (this.node.state.name === 'leader' || this._options.standby) {
            next();
        } else {
            setTimeout(next, this._options.waitLeaderTimeout);
            this.node.once('leader', next);
        }
    }

    _put(key, value, options, next) {
        this.node.command({
            type: 'put',
            key: key,
            value: value
        }, options, next);
    }

    _del(key, options, next) {
        this.node.command({
            type: 'del',
            key: key
        }, options, next);
    }

    _get(key, options, next) {
        // this is not a typical command, we do not mutate state here
        // therefore we wire the method right here
        return this._db.get(this.node.id, key, next);
    }

    _batch(operations, options, next) {
        this.node.command({
            type: 'batch',
            operations: operations,
            options: options
        }, options, next);
    }

    _close(callback) {
        this.node.close(err => {
            if (err) {
                if (typeof callback === 'function') {
                    callback(err);
                } else {
                    this.emit('error', err);
                }
            }

            this._db.close(callback);
        });
    }

}

export default SkiffClient;
