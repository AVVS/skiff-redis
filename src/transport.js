'use strict';

export function resolve(module) {
    if (typeof module === 'string') {
        module = require('skiff-' + module);
    }
    return module;
};
