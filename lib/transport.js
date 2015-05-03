'use strict';

Object.defineProperty(exports, '__esModule', {
    value: true
});
exports.resolve = resolve;
'use strict';

function resolve(module) {
    if (typeof module === 'string') {
        module = require('skiff-' + module);
    }
    return module;
}

;
