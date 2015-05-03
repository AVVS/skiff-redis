'use strict';

var Lab = require('lab');
var lab = exports.lab = Lab.script();
var describe = lab.describe;
var it = lab.it;
var assert = Lab.assertions;

var path = require('path');
var async = require('neo-async');
var concat = require('concat-stream');
var Redis = require('ioredis');

var Client = require('../');

describe('cluster', function() {
  var leader;
  var node;
  var redis = new Redis();
  var namespace = '{test-skiff}';

  it('removes keys', function(done) {
      redis.keys(namespace + '*')
        .then(redis.del.bind(redis))
        .catch(function (err) {
            return;
        })
        .nodeify(done);
  });

  var data = [];
  for (var i = 1 ; i <= 20; i ++) {
    data.push('data ' + i);
  }

  it('leader can get created', function(done) {
    leader = new Client(
      'tcp+msgpack://localhost:8100',
      {
        redis: redis,
        namespace: namespace,
        commandTimeout: 10e3,
        retainedLogEntries: 10
      });

    leader.once('leader', function() {
      done();
    });
  });

  it('leader can write', {timeout: 60e3}, function(done) {
    var i = 0;
    async.eachLimit(data, 5, function(d, cb) {
      i ++;
      leader.put(pack(i), d, cb);
    }, done);
  });

  it('leader has all the data', function(done) {
    leader.createReadStream().pipe(concat(read));

    function read(values) {
      assert.deepEqual(values.map(function(rec) {
        return rec.value;
      }), data);
      done();
    }
  });

  it('can join additional node that eventually syncs', {timeout: 10e3},
    function(done) {
      node = new Client(
        'tcp+msgpack://localhost:8103',
        {
          namespace: namespace,
          redis: redis,
          standby: true
        }
      );

      var count = 0;
      node.on('InstallSnapshot', function() {
        count ++;
        if (count == data.length) {
          setTimeout(function() {
            node.createReadStream().pipe(concat(read));
          }, 1e3);
        }
      });

      function read(values) {
        assert.deepEqual(values.map(function(rec) {
          return rec.value;
        }), data);
        done();
      }

      leader.join(node.id, function(err) {
        if (err) {
          throw err;
        }
      });

    }
  );
});

function pack(i) {
  var out = '';
  if (i < 100) {
    out += '0';
  }
  if (i < 10) {
    out += '0';
  }
  out += i;
  return out;
}