'use strict';

const assert = require('bsert');
const path = require('path');
const {tmpdir} = require('os');
const {randomBytes} = require('crypto');
const fs = require('fs');
const common = exports;

common.testdir = (name) => {
  assert(/^[a-z0-9\-]+$/.test(name), 'Invalid name');

  const uniq = randomBytes(4).toString('hex');
  return path.join(tmpdir(), `liburkel-node-test-${name}-${uniq}`);
};

common.serializeU32 = (num) => {
  assert((num >>> 0) === num);

  let str = num.toString(10);

  while (str.length < 10)
    str = '0' + str;

  return str;
};

common.isTreeDir = (dir, locked) => {
  const files = new Set(fs.readdirSync(dir));

  if (files.length < 2)
    return false;

  if (!files.has('meta'))
    return false;

  files.delete('meta');

  if (locked === true && !files.has('lock'))
    return false;

  if (locked === false && files.has('lock'))
    return false;

  files.delete('lock');

  let i = 1;
  while (files.size > 0) {
    const fname = common.serializeU32(i);

    if (!files.has(fname))
      break;

    files.delete(fname);
    i++;
  }

  return files.size === 0;
};

common.rmTreeDir = (dir) => {
  assert(common.isTreeDir(dir));

  const files = fs.readdirSync(dir);
  for (const file of files)
    fs.unlinkSync(path.join(dir, file));
  fs.rmdirSync(dir);
};

common.sleep = async (ms) => {
  return new Promise(r => setTimeout(r, ms));
};

common.randomKey = () => {
  return randomBytes(32);
};
