'use strict';

Object.defineProperty(exports, '__esModule', {
    value: true
});

var _interopRequireWildcard = function (obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj['default'] = obj; return newObj; } };

var _interopRequireDefault = function (obj) { return obj && obj.__esModule ? obj : { 'default': obj }; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

var _get = function get(_x2, _x3, _x4) { var _again = true; _function: while (_again) { desc = parent = getter = undefined; _again = false; var object = _x2,
    property = _x3,
    receiver = _x4; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x2 = parent; _x3 = property; _x4 = receiver; _again = true; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

var _inherits = function (subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _SkiffNode = require('skiff-algorithm');

var _SkiffNode2 = _interopRequireDefault(_SkiffNode);

var _SkiffRedis = require('./persistence.js');

var _SkiffRedis2 = _interopRequireDefault(_SkiffRedis);

// options

var _defaultOptions = require('./default_options');

var _defaultOptions2 = _interopRequireDefault(_defaultOptions);

var _EventEmitter2 = require('events');

var _import = require('lodash');

var _import2 = _interopRequireDefault(_import);

var _propagate = require('propagate');

var _propagate2 = _interopRequireDefault(_propagate);

var _async = require('neo-async');

var _async2 = _interopRequireDefault(_async);

var _Redis = require('ioredis');

var _Redis2 = _interopRequireDefault(_Redis);

var _import3 = require('./transport.js');

var transports = _interopRequireWildcard(_import3);

'use strict';

var beforeLeaderOps = ['listen', 'waitLeader', 'close'];

var SkiffClient = (function (_EventEmitter) {
    function SkiffClient(id) {
        var _this2 = this;

        var options = arguments[1] === undefined ? {} : arguments[1];

        _classCallCheck(this, SkiffClient);

        _get(Object.getPrototypeOf(SkiffClient.prototype), 'constructor', this).call(this);

        if (!id) {
            throw new Error('node id must be defined');
        }

        _import2['default'].bindAll(this, ['_listening', '_work']);

        this.id = id;

        this._options = _import2['default'].extend({}, _defaultOptions2['default'], options);

        var redisOpts = this._options.redisClient;

        if (!(redisOpts instanceof _Redis2['default'])) {
            if (Array.isArray(redisOpts)) {
                switch (redisOpts.length) {
                    case 0:
                    case 1:
                        this._options.redisClient = new _Redis2['default'](redisOpts[0]);
                        break;
                    case 2:
                        this._options.redisClient = new _Redis2['default'](redisOpts[0], redisOpts[1]);
                        break;
                    default:
                        this._options.redisClient = new _Redis2['default'](redisOpts[0], redisOpts[1], redisOpts[2]);
                        break;
                }
            } else {
                this._options.redisClient = new _Redis2['default'](redisOpts);
            }
        }

        this._db = new _SkiffRedis2['default']({ namespace: this._options.namespace, redis: this._options.redisClient });
        this._options.id = id;
        this._options.persistence = this._db;
        this._options.transport = new (transports.resolve(this._options.transport))();
        this.node = new _SkiffNode2['default'](this._options);

        _propagate2['default'](this.node, this);

        this.node.on('error', function (err) {
            _this2.emit('error', err);
        });

        // propagate certain persistence layer events
        this._options.persistence.on('error', function (err) {
            _this2.emit('error', err);
        });

        // propagate certain transport layer events
        this._options.transport.on('error', function (err) {
            _this2.emit('error', err);
        });

        // queue
        this.queue = _async2['default'].queue(this._work, 1);

        if (this._options.autoListen) {
            this.queue.push({ op: 'listen', args: [this._listening] });
        }

        this.queue.push({ op: 'waitLeader' });
    }

    _inherits(SkiffClient, _EventEmitter);

    _createClass(SkiffClient, [{
        key: 'put',

        // Public API

        value: function put(key, value) {
            for (var _len = arguments.length, opts = Array(_len > 2 ? _len - 2 : 0), _key = 2; _key < _len; _key++) {
                opts[_key - 2] = arguments[_key];
            }

            var callback = opts.pop();
            var options = opts.pop();

            this.queue.push({
                op: 'put',
                args: [key, value, options, callback]
            });
        }
    }, {
        key: 'del',
        value: function del(key) {
            for (var _len2 = arguments.length, opts = Array(_len2 > 1 ? _len2 - 1 : 0), _key2 = 1; _key2 < _len2; _key2++) {
                opts[_key2 - 1] = arguments[_key2];
            }

            var callback = opts.pop();
            var options = opts.pop();

            this.queue.push({
                op: 'del',
                args: [key, options, callback]
            });
        }
    }, {
        key: 'batch',
        value: function batch(ops) {
            for (var _len3 = arguments.length, opts = Array(_len3 > 1 ? _len3 - 1 : 0), _key3 = 1; _key3 < _len3; _key3++) {
                opts[_key3 - 1] = arguments[_key3];
            }

            var callback = opts.pop();
            var options = opts.pop();

            this.queue.push({
                op: 'batch',
                args: [ops, options, callback]
            });
        }
    }, {
        key: 'get',
        value: function get(key) {
            for (var _len4 = arguments.length, opts = Array(_len4 > 1 ? _len4 - 1 : 0), _key4 = 1; _key4 < _len4; _key4++) {
                opts[_key4 - 1] = arguments[_key4];
            }

            var callback = opts.pop();
            var options = opts.pop();

            this.queue.push({
                op: 'get',
                args: [key, options, callback]
            });
        }
    }, {
        key: 'iterator',
        value: function iterator(options) {}
    }, {
        key: 'createReadStream',
        value: function createReadStream(options) {
            return this._db._createReadStream(this.node.id, options);
        }
    }, {
        key: 'createWriteStream',
        value: function createWriteStream(options) {
            return this._db._createWriteStream(this.node.id, options);
        }
    }, {
        key: 'join',
        value: function join(node) {
            for (var _len5 = arguments.length, opts = Array(_len5 > 1 ? _len5 - 1 : 0), _key5 = 1; _key5 < _len5; _key5++) {
                opts[_key5 - 1] = arguments[_key5];
            }

            var callback = opts.pop();
            var metadata = opts.pop() || null;

            this.node.join(node, metadata, callback);
        }
    }, {
        key: 'leave',
        value: function leave(node, callback) {
            this.node.leave(node, callback);
        }
    }, {
        key: 'peerMeta',
        value: function peerMeta(node) {
            this.node.peerMeta(node);
        }
    }, {
        key: 'listen',
        value: function listen(callback) {
            var _this3 = this;

            this.node.listen(this.node.id, function (err) {
                if (err) {
                    if (typeof callback === 'function') {
                        callback(err);
                    } else {
                        _this3.emit('error', err);
                    }
                    return;
                }

                if (typeof callback === 'function') {
                    callback();
                }
            });
        }
    }, {
        key: 'close',
        value: function close(callback) {
            this.queue.push({
                op: 'close',
                args: [callback]
            });
        }
    }, {
        key: 'open',
        value: function open(callback) {
            this._waitLeader(callback);
        }
    }, {
        key: '_listening',

        // Private API

        value: function _listening(err) {
            if (err) {
                this.emit('error', err);
            }
        }
    }, {
        key: '_work',
        value: function _work(item, next) {
            var method = '_' + item.op;
            var args = item.args || [];
            var appCallback = args[args.length - 1];
            var typeofAppCallback = typeof appCallback;

            if (typeofAppCallback === 'function' || typeofAppCallback === 'undefined') {
                args.pop();
            } else {
                appCallback = undefined;
            }

            var callback = function callback() {
                if (appCallback) {
                    appCallback.apply(null, arguments);
                }

                setImmediate(next);
            };

            if (beforeLeaderOps.indexOf(item.op) == -1 && this.node.state.name !== 'leader') {
                var err = new Error(item.op + ' operation requires being the leader');
                err.leader = this.node.currentLeader();
                return callback(err);
            }

            args.push(callback);
            this[method].apply(this, args);
        }
    }, {
        key: '_listen',
        value: function _listen(callback) {
            this.listen(callback);
        }
    }, {
        key: '_waitLeader',
        value: function _waitLeader(next) {
            next = _import2['default'].once(next);

            if (this.node.state.name === 'leader' || this._options.standby) {
                next();
            } else {
                setTimeout(next, this._options.waitLeaderTimeout);
                this.node.once('leader', next);
            }
        }
    }, {
        key: '_put',
        value: function _put(key, value, options, next) {
            this.node.command({
                type: 'put',
                key: key,
                value: value
            }, options, next);
        }
    }, {
        key: '_del',
        value: function _del(key, options, next) {
            this.node.command({
                type: 'del',
                key: key
            }, options, next);
        }
    }, {
        key: '_get',
        value: function _get(key, options, next) {
            // this is not a typical command, we do not mutate state here
            // therefore we wire the method right here
            return this._db.get(this.node.id, key, next);
        }
    }, {
        key: '_batch',
        value: function _batch(operations, options, next) {
            this.node.command({
                type: 'batch',
                operations: operations,
                options: options
            }, options, next);
        }
    }, {
        key: '_close',
        value: function _close(callback) {
            var _this4 = this;

            this.node.close(function (err) {
                if (err) {
                    if (typeof callback === 'function') {
                        callback(err);
                    } else {
                        _this4.emit('error', err);
                    }
                }

                _this4._db.close(callback);
            });
        }
    }]);

    return SkiffClient;
})(_EventEmitter2.EventEmitter);

exports['default'] = SkiffClient;
module.exports = exports['default'];
