/*!
 * proof.js - Proof buffer with metadata.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */
'use strict';

const assert = require('bsert');
const {proofTypes} = require('./common');

const HASH_SIZE = 32;
const HASH_BITS = 256;
const EMPTY_PROOF = Buffer.alloc(4, 0x00);

class EncodingError extends Error {
  /**
   * Create an encoding error.
   * @constructor
   * @param {Number} offset
   * @param {String} reason
   */

  constructor(offset, reason, start) {
    super();

    this.type = 'EncodingError';
    this.name = 'EncodingError';
    this.code = 'ERR_ENCODING';
    this.message = `${reason} (offset=${offset}).`;

    if (Error.captureStackTrace)
      Error.captureStackTrace(this, start || EncodingError);
  }
}

class Proof {
  constructor() {
    this.type = proofTypes.TYPE_UNKNOWN;
    this.depth = 0;
    this.nodesLen = 0;
    this.raw = EMPTY_PROOF;
  }

  getSize() {
    return this.raw.length;
  }

  write(data, off) {
    checkWrite(off + this.raw.length <= data.length, off);
    this.raw.copy(data, off);
    return off + this.raw.length;
  }

  read(data, off) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);

    let pos = off;

    const field = readU16(data, pos);
    pos += 2;
    this.type = field >>> 14;
    this.depth = field & ~(3 << 14);

    if (this.depth > HASH_BITS)
      throw new EncodingError(pos, 'Invalid depth');

    checkRead(pos + 2 <= data.length, pos);

    const count = readU16(data, pos);
    const bsize = (count + 7) >>> 3;
    pos += 2;

    if (count > HASH_BITS)
      throw new EncodingError(pos, 'Proof too large');

    checkRead(pos + bsize <= data.length, pos);
    pos += bsize;

    for (let i = 0; i < count; i++) {
      checkRead(pos + 2 <= data.length, pos);

      if (hasBit(data, (off + 4) * 8 + i))
        pos = skipReadBits(data, pos);

      pos += HASH_SIZE;
    }

    this.nodesLen = count;

    switch (this.type) {
      case proofTypes.TYPE_DEADEND: {
        break;
      }

      case proofTypes.TYPE_SHORT: {
        pos = skipReadBits(data, pos);
        pos += HASH_SIZE;
        pos += HASH_SIZE;
        break;
      }

      case proofTypes.TYPE_COLLISION: {
        pos += HASH_BITS >>> 3;
        pos += HASH_SIZE;
        break;
      }

      case proofTypes.TYPE_EXISTS: {
        checkRead(pos + 2 <= data.length, pos);
        const size = readU16(data, pos);
        pos += 2;
        pos += size;

        break;
      }

      default: {
        throw new Error('Invalid type.');
      }
    }

    this.raw = data.slice(off, pos);
    return pos;
  }

  encode() {
    return this.raw;
  }

  decode(data) {
    this.read(data, 0);
    return this;
  }

  readBR(br) {
    assert(br && typeof br.readU8 === 'function');
    br.offset = this.read(br.data, br.offset);
    return this;
  }

  writeBW(bw, hash, bits) {
    assert(bw && typeof bw.writeU8 === 'function');
    if (bw.data)
      bw.offset = this.write(bw.data, bw.offset, hash, bits);
    else
      bw.writeBytes(this.encode(hash, bits));
    return bw;
  }

  static read(data, off) {
    return new this().read(data, off);
  }

  static readBR(br) {
    return new this().readBR(br);
  }

  static decode(data) {
    return new this().decode(data);
  }

  static isProof(obj) {
    return obj instanceof this;
  }
}

function skipReadBits(data, off) {
  checkRead(off + 2 <= data.length, off);

  let size = data[off];
  off += 1;

  if (size & 0x80) {
    size -= 0x80;
    size *= 0x100;
    size += data[off];
    off += 1;
  }

  const bytes = (size + 7) >>> 3;

  checkRead(off + bytes <= data.length, off);

  return off + bytes;
}

function hasBit(key, index) {
  const oct = index >>> 3;
  const bit = index & 7;
  return (key[oct] >>> (7 - bit)) & 1;
}

function readU16(data, off) {
  return data[off++] + data[off] * 0x100;
}

function checkWrite(ok, offset, start) {
  if (!ok) {
    throw new EncodingError(offset,
      'Out of bounds write',
      start || checkWrite);
  }
}

function checkRead(ok, offset, start) {
  if (!ok) {
    throw new EncodingError(offset,
      'Out of bounds read',
      start || checkRead);
  }
}

module.exports = Proof;
