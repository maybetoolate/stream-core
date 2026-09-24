'use strict';
const { Readable } = require('./readable');
const { Writable } = require('./writable');
const { Transform } = require('./transform');
const { pipeline } = require('./pipeline');

module.exports = { Readable, Writable, Transform, pipeline };
