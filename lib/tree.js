/**
 * tree.js - JS Wrapper for the nurkel.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/liburkel
 */
'use strict';

const assert = require('bsert');
const path = require('path');
const nurkel = require('loady')('nurkel', __dirname);

/**
 * @typedef {Buffer} Hash
 */

/*
 * Compat
 */

const asyncIterator = Symbol.asyncIterator || 'asyncIterator';

// Placeholder methods wont be implemented.
const ERR_PLACEHOLDER = 'Placeholder method.';
const ERR_NOT_IMPL = 'Not implemented.';
const ERR_INIT = 'Database has already been initialized.';
const ERR_NOT_INIT = 'Database has not been initialized.';
const ERR_OPEN = 'Database is already open.';
const ERR_CLOSED = 'Database is already closed.';

class Tree {
  constructor(options) {
    this.options = new TreeOptions(options);
    this.prefix = this.options.prefix;
    this.path = path.join(this.prefix, 'tree');

    this.isOpen = false;
    this.hash = null;
    this.tree = null;
  }

  get bits() {
    return 256;
  }

  get hashSize() {
    return 32;
  }

  get keySize() {
    return 32;
  }

  /**
   * Check hash.
   * @param {Buffer} hash
   * @returns {Boolean}
   */

  isHash(hash) {
    if (!Buffer.isBuffer(hash))
      return false;
    return hash.length === this.size;
  }

  /**
   * Check key.
   * @param {Buffer} key
   * @returns {Boolean}
   */

  isKey(key) {
    if (!Buffer.isBuffer(key))
      return false;

    // TODO: Double check
    return key.length === (this.bits >>> 3);
  }

  /**
   * Check value.
   * @param {Buffer} value
   * @returns {Boolean}
   */

  isValue(value) {
    if (!Buffer.isBuffer(value))
      return false;

    return value.length < 0x400;
  }

  /**
   * Initialize database
   * @returns Tree
   */

  init() {
    assert(!this.tree, ERR_INIT);

    this.tree = nurkel.init();
    return this;
  }

  /**
   * Open tree.
   * @param {Hash} rootHash
   * @returns {Promise}
   */

  async open(rootHash) {
    assert(!this.isOpen, ERR_OPEN);
    assert(!rootHash, ERR_NOT_IMPL);

    this.init();

    this.hash = await nurkel.open(this.tree, this.path);
    this.isOpen = true;

    if (rootHash)
      await this.inject(rootHash);
  }

  /**
   * Close tree.
   * @returns {Promise}
   */

  async close() {
    assert(this.tree, ERR_NOT_INIT);
    assert(this.isOpen, ERR_CLOSED);

    await nurkel.close(this.tree);
    this.isOpen = false;
    this.hash = null;
  }

  /**
   * Get current root hash.
   * NOTE: This can only be achieved if we have another abstraction from the
   * NAPI for the Nodes. I don't think it's necessary. This method is here for
   * API compatibility purposes.
   * @throws {Error}
   */

  async getRoot() {
    throw new Error(ERR_PLACEHOLDER);
  }

  /**
   * Get root hash
   * @returns {Buffer}
   */

  rootHash() {
    assert(this.isOpen, ERR_CLOSED);
    return this.hash;
  }

  /**
   * Get value by key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  async get() {
    assert(this.tree, ERR_NOT_INIT);
    assert(this.isOpen, ERR_CLOSED);

    throw new Error(ERR_NOT_IMPL);
  }

  /**
   * Inject new root into the tree.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async inject(hash) {
    throw new Error(ERR_NOT_IMPL);
  }

  /**
   * Generate proof for the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async prove() {
    throw new Error(ERR_NOT_IMPL);
  }

  /**
   * Verify proof.
   * @param {Buffer} proof
   * @returns {Promise<Boolean>}
   */

  async verify(proof) {
    throw new Error(ERR_NOT_IMPL);
  }

  /**
   * Compact database
   * @returns {Promise}
   */

  async compact() {
    throw new Error(ERR_NOT_IMPL);
  }

  /**
   * Get new transaction instance
   * @returns {Transaction}
   */

  transaction() {
    return new Transaction(this);
  }

  /**
   * Get snapshot
   * @param {Hash} hash
   * @returns {Snapshot}
   */

  snapshot(hash) {
    throw new Error(ERR_NOT_IMPL);
  }

  /**
   * Get new transaction instance
   * @returns {Transaction}
   */

  batch() {
    return this.transaction();
  }

  /**
   * Get new transaction instance
   * @returns {Transaction}
   */

  txn() {
    return this.transaction();
  }
}

class TreeOptions {
  constructor(options) {
    this.prefix = '/';

    this.fromOptions(options);
  }

  fromOptions(options) {
    assert(options, 'Tree requires options.');
    assert(typeof options.prefix === 'string',
      'options.prefix must be a string.');

    this.prefix = options.prefix;
  }
}

/**
 * Snapshot is same as tx, just with different API.
 *
 * NOTE: isOpen wont always be consistent with
 * NAPI tx state. If Tree closes, it will also
 * close associated transactions/snapshots. But
 * it's not a problem, because that should not happen
 * in general. Also even if the calls to tx happen
 * they will just throw. (even reopen would fail, because
 * tree is closed)
 */

class Snapshot {
  constructor(tree, hash) {
    this.tree = tree;
    this.hash = hash;

    this.initialized = false;
    this.isOpen = false;
    this.tx = null;

    this.init();
  }

  /**
   * Initialize transaction
   */

  init() {
    assert(this.initialized === false);
    this.initialized = true;
    this.tx = nurkel.tx_init(this.tree.tree);
  }

  /**
   * Maybe open.
   * @returns {Promise}
   */

  async maybeOpen() {
    if (this.isOpen)
      return;

    await nurkel.tx_open(this.tx, this.hash);
    this.isOpen = true;
  }

  /**
   * Close the snapshot
   * @returns {Promise}
   */

  async close() {
    if (!this.isOpen)
      return;

    await nurkel.tx_close(this.tx);
    return;
  }

  rootHash() {
    return this.hash;
  }

  /**
   * Root wont be returned, here for API compatibility.
   * @throws {Error}
   */

  async getRoot() {
    throw new Error(ERR_PLACEHOLDER);
  }

  /**
   * Inject new root to the snapshot.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async inject(hash) {
    await this.maybeOpen();
    throw new Error(ERR_NOT_IMPL);
    // await nurkel.tx_inject(this.tx, hash);
    // this.hash = hash;
  }

  /**
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Buffer} - value
   */

  async get(key) {
    await this.maybeOpen();
    throw new Error(ERR_NOT_IMPL);
    // await nurkel.tx_get(this.tx, key);
  }

  /**
   * Get proof for the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async prove(key) {
    await this.maybeOpen();
    throw new Error(ERR_NOT_IMPL);
    // await nurkel.tx_prove(this.tx, key);
  }

  iterator(read = true) {
    const iter = new Iterator(this.tree, this, read);
    iter.root = this.root;
    return iter;
  }

  [asyncIterator]() {
    return this.entries();
  }

  keys() {
    const iter = this.iterator(false);
    return iter.keys();
  }

  values() {
    const iter = this.iterator(true);
    return iter.values();
  }

  entries() {
    const iter = this.iterator(true);
    return iter.entries();
  }
}

class Transaction extends Snapshot {
  constructor(tree) {
    assert(tree instanceof Tree);
    super(tree, tree.rootHash());
  }

  rootHash() {
    throw new Error(ERR_NOT_IMPL);
  }

  async getRoot() {
    throw new Error(ERR_NOT_IMPL);
  }

  async insert(key, value) {
    throw new Error(ERR_NOT_IMPL);
  }

  async remove(key) {
    throw new Error(ERR_NOT_IMPL);
  }

  async commit() {
    throw new Error(ERR_NOT_IMPL);
  }

  clear() {
    throw new Error(ERR_NOT_IMPL);
  }
}

class Iterator {
  constructor(tree, parent, read) {
    assert(tree instanceof Tree);
    assert(parent && typeof parent.getRoot === 'function');
    assert(typeof read === 'boolean');

    throw new Error(ERR_NOT_IMPL);
  }

  [asyncIterator]() {
    return this.entries();
  }

  keys() {
    return new AsyncIterator(this, 0);
  }

  values() {
    return new AsyncIterator(this, 1);
  }

  entries() {
    return new AsyncIterator(this, 2);
  }

  push(node, depth) {
    throw new Error(ERR_NOT_IMPL);
  }

  pop() {
    throw new Error(ERR_NOT_IMPL);
  }

  top() {
    throw new Error(ERR_NOT_IMPL);
  }

  length() {
    throw new Error(ERR_NOT_IMPL);
  }

  async seek() {
    throw new Error(ERR_NOT_IMPL);
  }

  async next() {
    throw new Error(ERR_NOT_IMPL);
  }
}

class AsyncIterator {
  constructor(iter, type) {
    throw new Error(ERR_NOT_IMPL);
  }

  async next() {
    throw new Error(ERR_NOT_IMPL);
  }
}

exports.Tree = Tree;
