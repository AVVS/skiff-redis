'use strict';

var Lab = require('lab');
var lab = exports.lab = Lab.script();
var describe = lab.describe;
var it = lab.it;
var assert = Lab.assertions;

var path = require('path');
var async = require('neo-async');
var Redis = require('ioredis');
var Client = require('../');

var domain = require('domain');

describe('resilient clients', function() {
  var leader;
  var nodes = [];

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


  it('leader can get created', function(done) {
    leader = new Client(
      'tcp+msgpack://localhost:8090',
      {
        redis: redis,
        namespace: namespace,
        commandTimeout: 10e3
      });

    leader.once('leader', function() {
      done();
    });
  });

  it('other nodes can get created', function(done) {
    var d = domain.create();
    d.on('error', function(err) {
      console.error(err.stack);
    });
    d.run(function() {
      var port = 8090;
      var node;
      for (var i = 1 ; i <= 4 ; i ++) {
        node = new Client(
          'tcp+msgpack://localhost:' + (port + i),
          {
            redis: redis,
            namespace: namespace,
            standby: true
          }
        );
        nodes.push(node);
      }
      done();
    });
  });

  it('leader can join other nodes', {timeout: 4e3}, function(done) {
    async.eachSeries(nodes, function(node, cb) {
      leader.join(node.id, cb);
    }, function(err) {
      if (err) {
        throw err;
      }
      done();
    });
  });

  it('some time passes by...', {timeout: 4e3}, function(done) {
    setTimeout(done, 3e3);
  });

  it('2 nodes can quit...', {timeout: 6e3}, function(done) {
    var quitting = nodes.splice(2, 2);
    async.each(quitting, quit, done);

    function quit(node, cb) {
      node.close(cb);
    }
  });

  it('some time passes by...', {timeout: 4e3}, function(done) {
    setTimeout(done, 3e3);
  });

  it('... and the cluster still writes', function(done) {
    leader.put('a', 'b', done);
  });

  it('another node dies...', function(done) {
    nodes.shift().close(done);
  });

  it('some time passes by...', {timeout: 4e3}, function(done) {
    setTimeout(done, 3e3);
  });

  it('... and the cluster no longer writes', {timeout: 6e3}, function(done) {
    leader.put('c', 'd', {timeout: 3e3}, function(err) {
      assert.ok(err instanceof Error);
      assert(err.message.indexOf('timedout') > -1);
      done();
    });
  });

  it('if I revive a node...', function(done) {
    new Client(
      'tcp+msgpack://localhost:8091',
      {
        redis: redis,
        namespace: namespace,
        standby: true
      }
    );
    done();
  });

  it('some time passes by...', {timeout: 4e3}, function(done) {
    setTimeout(done, 3e3);
  });

  it('... the cluster can write again', {timeout: 31e3}, function(done) {
    leader.put('c', 'd', {timeout: 30e3}, done);
  });

});