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

describe('standalone client', function() {
  var client;
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

  it('can get created', function(done) {
    client = new Client('tcp+msgpack://localhost:8050', { redis: redis, namespace: namespace });
    done();
  });

  it('emits the leader event', {timeout: 10e3}, function(done) {
    client.once('leader', function() {
      done();
    });
  });

  it('allows putting', {timeout: 4e3}, function(done) {
    client.put('key', 'value', done);
  });

  it('allows getting', {timeout: 4e3}, function(done) {
    client.get('key', function(err, value) {

      if (err) {
        throw err;
      }

      assert.equal(value, 'value');
      done();
    });
  });

  it('allows batching', function(done) {
    client.batch([
      {type: 'put', key: 'key a', value: 'value a'},
      {type: 'put', key: 'key b', value: 'value b'},
      {type: 'put', key: 'key c', value: 'value c'},
      {type: 'put', key: 'key c', value: 'value c2'},
      {type: 'del', key: 'key a'}
      ], done);
  });

  it('batch worked', function(done) {
    async.map(['key b', 'key c'], client.get.bind(client), resulted);

    function resulted(err, results) {
      if (err) {
        throw err;
      }
      assert.deepEqual(results, ['value b', 'value c2']);

      client.get('key a', function(err) {
        assert(err && err.notFound);
        done();
      });
    }
  });

  it('can create a read stream with no args', function(done) {
    client.createReadStream().pipe(concat(function(values) {
      assert.deepEqual(values, [
        {key: 'key', value: 'value'},
        {key: 'key b', value: 'value b'},
        {key: 'key c', value: 'value c2'}
        ]);
      done();
    }));
  });

  it('can create a read stream with some args', function(done) {
    client.createReadStream({
      gte: 'key b',
      lte: 'key c'
    }).pipe(concat(function(values) {
      assert.deepEqual(values, [
        {key: 'key b', value: 'value b'},
        {key: 'key c', value: 'value c2'}
        ]);
      done();
    }));
  });

  it('can create a write stream', function(done) {
    var ws = client.createWriteStream();

    ws.write({
      type: 'put',
      key: 'key d',
      value: 'value d'
    });
    ws.write({
      type: 'put',
      key: 'key e',
      value: 'value e'
    });
    ws.end({
      type: 'put',
      key: 'key f',
      value: 'value f'
    });

    ws.once('finish', done);
  });

  it('write stream worked', function(done) {
    client.createReadStream({
      gte: 'key d',
      lte: 'key f'
    }).pipe(concat(function(values) {
      assert.deepEqual(values, [
        {key: 'key d', value: 'value d'},
        {key: 'key e', value: 'value e'},
        {key: 'key f', value: 'value f'}
        ]);
      done();
    }));
  });

  it('closes', function(done) {
    client.close(done);
  });
});