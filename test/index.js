'use strict';

var test = require('abstract-skiff-persistence/test/all');

var Redis = require('ioredis');
var redis = new Redis();
var SkiffRedisPersistence = require('../lib');
var p = new SkiffRedisPersistence({ redis: redis });

var options = {
  commands: [
    {type: 'put', key: 'a', value: 1},
    {type: 'put', key: 'b', value: 2},
    {type: 'del', key: 'a'},
    {type: 'put', key: 'c', value: {some: 'object'}}
  ],
  expectedReads: [
    {key: 'b', value: 2},
    {key: 'c', value: {some: 'object'}}
  ],
  newWrites: [
    {type: 'put', key: 'd', value: 3},
    {type: 'put', key: 'e', value: {some: 'other object'}},
    {type: 'put', key: 'f', value: 'some other string'}
  ],
  newReads: [
    {key: 'd', value: 3},
    {key: 'e', value: {some: 'other object'}},
    {key: 'f', value: 'some other string'}
  ]
};

test(p, options);