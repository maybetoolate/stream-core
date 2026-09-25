'use strict';
const { FileReadable, FileWritable } = require('./file');
const { SocketReadable, SocketWritable } = require('./socket');
const { StdinReadable, StdoutWritable } = require('./stdio');

module.exports = {
  FileReadable,
  FileWritable,
  SocketReadable,
  SocketWritable,
  StdinReadable,
  StdoutWritable,
};
