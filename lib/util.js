'use strict';

const assert = require('bsert');

function randomString() {
  const m = Number.MAX_SAFE_INTEGER;
  const n = Math.random() * m;
  const s = Math.floor(n);
  return s.toString(32);
}

function randomPath(path) {
  assert(typeof path === 'string');

  while (path.length > 1) {
    const ch = path[path.length - 1];

    if (ch !== '/' && ch !== '\\')
      break;

    path = path.slice(0, -1);
  }

  return `${path}.${randomString()}~`;
}

exports.randomString = randomString;
exports.randomPath = randomPath;
