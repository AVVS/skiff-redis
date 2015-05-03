# skiff-redis

[![Build Status](https://travis-ci.org/AVVS/skiff-redis.svg?branch=master)](https://travis-ci.org/AVVS/skiff-redis)

Implementation of redis-backed skiff persistence layer.
Original abstract class is located here: https://github.com/pgte/abstract-skiff-transport

Library to be used with is here: https://github.com/pgte/skiff

This can not be extended with level-* plugins as it relies on a different store.
If you want to use it with redisDOWN - look at the skiff-level with appropriate backend.
