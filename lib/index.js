'use strict';

Object.defineProperty(exports, '__esModule', {
    value: true
});

var _interopRequireDefault = function (obj) { return obj && obj.__esModule ? obj : { 'default': obj }; };

var _slicedToArray = function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i['return']) _i['return'](); } finally { if (_d) throw _e; } } return _arr; } else { throw new TypeError('Invalid attempt to destructure non-iterable instance'); } };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

var _get = function get(_x2, _x3, _x4) { var _again = true; _function: while (_again) { desc = parent = getter = undefined; _again = false; var object = _x2,
    property = _x3,
    receiver = _x4; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x2 = parent; _x3 = property; _x4 = receiver; _again = true; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

var _inherits = function (subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _msgpack5 = require('msgpack5');

var _msgpack52 = _interopRequireDefault(_msgpack5);

var _SkiffPersistence2 = require('abstract-skiff-persistence');

var _SkiffPersistence3 = _interopRequireDefault(_SkiffPersistence2);

var _import = require('lodash');

var _import2 = _interopRequireDefault(_import);

var _async = require('neo-async');

var _async2 = _interopRequireDefault(_async);

var _Writable2 = require('readable-stream');

var _From = require('from');

var _From2 = _interopRequireDefault(_From);

'use strict';

var msgpack = _msgpack52['default']();

function encode(val) {
    return msgpack.encode(val).slice();
}

function decode(buf) {
    return msgpack.decode(buf);
}

var SkiffRedisWritable = (function (_Writable) {
    function SkiffRedisWritable(stateKey, redis) {
        _classCallCheck(this, SkiffRedisWritable);

        _get(Object.getPrototypeOf(SkiffRedisWritable.prototype), 'constructor', this).call(this, { objectMode: true });
        this._key = stateKey;
        this._redis = redis;
        this._redis.on('error', this._onError);
        this.on('finish', this.destroy);
    }

    _inherits(SkiffRedisWritable, _Writable);

    _createClass(SkiffRedisWritable, [{
        key: '_onError',
        value: function _onError(err) {
            this.emit('error', err);
            this.destroy();
        }
    }, {
        key: '_write',
        value: function _write(operation, enc, next) {
            switch (operation.type) {
                case 'del':
                    this._redis.hdel(this._key, operation.key, next);
                    break;

                case 'put':
                    this._redis.hset(this._key, operation.key, encode(operation.value), next);
                    break;
            }
        }
    }, {
        key: 'destroy',
        value: function destroy() {
            this.removeListener('finish', this.destroy);
            this._redis.removeListener('error', this._onError);
            this._redis = null;
            this._key = null;
        }
    }]);

    return SkiffRedisWritable;
})(_Writable2.Writable);

var SkiffRedis = (function (_SkiffPersistence) {
    function SkiffRedis() {
        var options = arguments[0] === undefined ? {} : arguments[0];

        _classCallCheck(this, SkiffRedis);

        _get(Object.getPrototypeOf(SkiffRedis.prototype), 'constructor', this).call(this, options);

        var redis = options.redis;
        var namespace = options.namespace;

        // in case we use redis cluster, make sure that namespace is hashed
        this._namespace = namespace || '{skiff-redis}';

        this._redis = redis;

        this._redis.on('error', this._onError);

        this.nodes = {};
    }

    _inherits(SkiffRedis, _SkiffPersistence);

    _createClass(SkiffRedis, [{
        key: '_onError',
        value: function _onError(err) {
            this.emit('error', err);
        }
    }, {
        key: '_key',
        value: function _key() {
            for (var _len = arguments.length, parts = Array(_len), _key = 0; _key < _len; _key++) {
                parts[_key] = arguments[_key];
            }

            if (parts.length === 0) {
                throw new Error('parts must include at least one value');
            }

            return this._namespace + parts.join('~');
        }
    }, {
        key: '_saveMeta',
        value: function _saveMeta(nodeId, state, callback) {
            var redis = this._redis;
            var pipeline = redis.pipeline();
            var log = state.log;
            var logEntries = log.entries;

            // do not enter hashtable mode
            state.log = _import2['default'].omit(log, ['entries']);

            pipeline.set(this._key(nodeId, 'meta'), encode(state));

            var minLogIndex = logEntries.length && logEntries[0].index || 0;
            var maxLogIndex = logEntries.length && logEntries[logEntries.length - 1].index || Infinity;
            var logNamespace = this._key(nodeId, 'logs');
            var cursor = 0;
            var maxReadIndex = 0;

            _async2['default'].doUntil(function zscanRedis(next) {
                redis.zscanBuffer(logNamespace, cursor, 'count', 10, function zscanResponse(err, response) {
                    if (err) {
                        return next(err);
                    }

                    // update cursor
                    cursor = parseInt(response[0], 10);

                    // process log entries
                    var entries = response[1];
                    for (var i = 0, l = entries.length; i < l; i += 2) {
                        var index = parseInt(entries[i + 1], 10);
                        var entry = entries[i];

                        if (index > maxReadIndex) {
                            maxReadIndex = index;
                        }

                        if (index < minLogIndex || index > maxLogIndex) {
                            pipeline.zrem(logNamespace, entry);
                        } else {
                            var correspondingNewEntry = logEntries[index - minLogIndex];
                            var decodedEntry = decode(entry);

                            if (correspondingNewEntry.uuid !== decodedEntry.uuid) {
                                pipeline.zrem(logNamespace, entry);
                                pipeline.zadd(logNamespace, correspondingNewEntry.index, encode(correspondingNewEntry));
                            }
                        }
                    }

                    next();
                });
            }, function isIterationComplete() {
                return cursor === 0;
            }, function saveMeta(err) {
                if (err) {
                    return callback(err);
                }

                if (maxReadIndex < maxLogIndex) {
                    logEntries.slice(maxReadIndex - minLogIndex + 1).forEach(function (entry) {
                        pipeline.zadd(logNamespace, entry.index, encode(entry));
                    });
                }

                pipeline.exec(callback);
            });
        }
    }, {
        key: '_loadMeta',
        value: function _loadMeta(nodeId, callback) {

            this._redis.pipeline().getBuffer(this._key(nodeId, 'meta')).zrangebyscoreBuffer(this._key(nodeId, 'logs'), '-inf', '+inf').exec(function (___, results) {
                // err is always null

                var _results$0 = _slicedToArray(results[0], 2);

                var err = _results$0[0];
                var meta = _results$0[1];

                var _results$1 = _slicedToArray(results[1], 2);

                var logerr = _results$1[0];
                var logEntries = _results$1[1];

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
    }, {
        key: '_applyCommand',
        value: function _applyCommand(nodeId, commitIndex, command, callback) {
            var stateKey = this._key(nodeId, 'state');
            var commitKey = this._key(nodeId, 'commitIndex');
            var pipeline = this._redis.pipeline();

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
    }, {
        key: '_lastAppliedCommitIndex',
        value: function _lastAppliedCommitIndex(nodeId, callback) {
            this._redis.get(this._key(nodeId, 'commitIndex')).then(Number).nodeify(callback);
        }
    }, {
        key: '_saveCommitIndex',
        value: function _saveCommitIndex(nodeId, commitIndex, callback) {
            this._redis.set(this._key(nodeId, 'commitIndex'), commitIndex, callback);
        }
    }, {
        key: '_createReadStream',
        value: function _createReadStream(nodeId) {
            var stateKey = this._key(nodeId, 'state');
            var redis = this._redis;
            var cursor = 0;
            var readable = _From2['default'](function getChunk(count, next) {
                var _this2 = this;

                redis.hscanBuffer(stateKey, cursor, 'count', count || 10, function (err, response) {
                    if (err) {
                        _this2.emit('error', err);
                        _this2.destroy();
                        return next();
                    }

                    cursor = parseInt(response[0], 10);

                    // hash
                    var sets = response[1];

                    for (var i = 0, l = sets.length; i < l; i += 2) {
                        var hashKey = sets[i];
                        var hashValue = decode(sets[i + 1]);

                        _this2.emit('data', { key: hashKey.toString(), value: hashValue });
                    }

                    if (cursor === 0) {
                        _this2.emit('end');
                    }

                    next();
                });
            });

            return readable;
        }
    }, {
        key: '_createWriteStream',
        value: function _createWriteStream(nodeId) {
            return new SkiffRedisWritable(this._key(nodeId, 'state'), this._redis);
        }
    }, {
        key: '_removeAllState',
        value: function _removeAllState(nodeId, callback) {
            this._redis.del(this._key(nodeId, 'state'), callback);
        }
    }, {
        key: '_close',
        value: function _close(callback) {
            this._redis.disconnect();
            this._redis.removeListener('error', this._onError);
            callback();
        }
    }]);

    return SkiffRedis;
})(_SkiffPersistence3['default']);

exports['default'] = SkiffRedis;
module.exports = exports['default'];
