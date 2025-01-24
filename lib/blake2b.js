/*!
 * blake2b.js - BLAKE2b bindings
 * Copyright (c) 2025, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

const assert = require('bsert');
const common = require('./common');
const nurkel = require('./nurkel');

const NULL = Buffer.alloc(0);

class BLAKE2b {
  constructor() {
    this._ctx = nurkel.blake2b_create();
  }

  /**
   * @param {Number} [size=32]
   * @param {Buffer} [key=null]
   * @returns {this}
   */

  init(size, key) {
    if (size == null)
      size = common.HASH_SIZE;

    if (key == null)
      key = NULL;

    assert((size >>> 0) === size, 'Size must be a number.');
    assert(Buffer.isBuffer(key), 'Key must be a buffer.');

    nurkel.blake2b_init(this._ctx, size, key);
    return this;
  }

  /**
   * Update hash with data.
   * @param {Buffer} data
   * @returns {this}
   */

  update(data) {
    assert(Buffer.isBuffer(data));
    assert(this._ctx, 'Not initialized.');

    nurkel.blake2b_update(this._ctx, data);
    return this;
  }

  /**
   * Finalize hash.
   * @returns {Buffer}
   */

  final() {
    assert(this._ctx, 'Not initialized.');

    return nurkel.blake2b_final(this._ctx);
  }

  static hash() {
    return new BLAKE2b();
  }

  /**
   * Hash data with BLAKE2b.
   * @param {Buffer} data
   * @param {Number} [size=32]
   * @param {Buffer} [key=null]
   * @returns {Buffer}
   */

  static digest(data, size, key) {
    const {ctx} = BLAKE2b;

    ctx.init(size, key);
    ctx.update(data);

    return ctx.final();
  }

  /**
   * @param {Buffer} x
   * @param {Number} y
   * @param {Buffer} [z]
   * @param {Number} [size=32]
   * @param {Buffer} [key=null]
   * @returns {Buffer}
   */

  static multi(x, y, z, size, key) {
    const {ctx} = BLAKE2b;

    ctx.init(size, key);
    ctx.update(x);
    ctx.update(y);

    if (z)
      ctx.update(z);

    return ctx.final();
  }
}

BLAKE2b.native = 1;
BLAKE2b.id = 'BLAKE2B256';
BLAKE2b.size = 32;
BLAKE2b.bits = 256;
BLAKE2b.blockSize = 128;
BLAKE2b.zero = Buffer.alloc(32, 0x00);
BLAKE2b.ctx = new BLAKE2b();

module.exports = BLAKE2b;
