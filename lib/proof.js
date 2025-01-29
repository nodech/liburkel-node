/*!
 * proof.js - Proof buffer with metadata.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */
'use strict';

const BLAKE2b = require('./blake2b');
const Proof = require('urkel/lib/proof');

const HASH_BITS = 256;

class WrappedProof {
  constructor() {
    this._proof = new Proof();
    this._raw = null;
    this._size = null;
  }

  get type() {
    return this._proof.type;
  }

  /**
   * @returns {Number}
   */

  getSize() {
    if (!this._size)
      this._size = this._proof.getSize(BLAKE2b, HASH_BITS);

    return this._size;
  }

  /**
   * @param {Buffer} data
   * @param {Number} off
   * @returns {Number}
   */

  write(data, off) {
    return this._proof.write(data, off, BLAKE2b, HASH_BITS);
  }

  /**
   * @param {bio.BufferWriter|bio.StaticWriter} bw
   * @returns {bio.BufferWriter|bio.StaticWriter}
   */

  writeBW(bw) {
    return this._proof.writeBW(bw, BLAKE2b, HASH_BITS);
  }

  /**
   * @param {Buffer} data
   * @param {Number} off
   * @returns {Number}
   */

  read(data, off) {
    const pos = this._proof.read(data, off, BLAKE2b, HASH_BITS);
    this._raw = data.slice(off, pos);
    this._size = pos - off;
    return this;
  }

  /**
   * @param {BufferReader} br
   * @returns {this}
   */

  readBR(br) {
    this._raw = null;
    this._size = null;
    this._proof.readBR(br, BLAKE2b, HASH_BITS);
    return this;
  }

  /**
   * @returns {Buffer}
   */

  encode() {
    if (!this._raw)
      this._raw = this._proof.encode(BLAKE2b, HASH_BITS);

    return this._raw;
  }

  /**
   * @param {Buffer} data
   * @returns {this}
   */

  decode(data) {
    this.read(data, 0);
    return this;
  }

  /**
   * Verify
   * @param {Buffer} root
   * @param {Buffer} key
   * @returns {Array}
   */

  verify(root, key) {
    return this._proof.verify(root, key, BLAKE2b, HASH_BITS);
  }

  /**
   * Clear cached values.
   * @returns {this}
   */

  refresh() {
    this._raw = null;
    this._size = null;
    return this;
  }

  /**
   * @param {Buffer} data
   * @param {Number} off
   * @returns {WrappedProof}
   */

  static read(data, off) {
    return new this().read(data, off);
  }

  /**
   * @param {BufferReader} br
   * @returns {WrappedProof}
   */

  static readBR(br) {
    return new this().readBR(br);
  }

  /**
   * @param {Buffer} data
   * @returns {WrappedProof}
   */

  static decode(data) {
    return new this().decode(data);
  }

  /**
   * @param {*} obj
   * @returns {Boolean}
   */

  static isProof(obj) {
    return obj instanceof this;
  }
}

module.exports = WrappedProof;
