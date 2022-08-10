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

/*
 * Errors
 */

// Placeholder methods wont be implemented.
const ERR_PLACEHOLDER = 'Placeholder method.';
const ERR_NOT_IMPL = 'Not implemented.';
const ERR_NOT_SUPPORTED = 'Not supported.';

const ERR_INIT = 'Database has already been initialized.';
const ERR_NOT_INIT = 'Database has not been initialized.';
const ERR_OPEN = 'Database is already open.';
const ERR_CLOSED = 'Database is already closed.';

const ERR_TX_OPEN = 'Transaction is already open.';
const ERR_TX_NOT_OPEN = 'Transaction is not open.';
const ERR_TX_NOT_FLUSHED = 'Transaction is not flushed.';

/*
 * Proof constants
 */

const URKEL_OK = 0;
const URKEL_EHASHMISMATCH = 1;
const URKEL_ESAMEKEY = 2;
const URKEL_ESAMEPATH = 3;
const URKEL_ENEGDEPTH = 4;
const URKEL_EPATHMISMATCH = 5;
const URKEL_ETOODEEP = 6;
const URKEL_EINVAL = 7;
const URKEL_ENOTFOUND = 8;
const URKEL_ECORRUPTION = 9;
const URKEL_ENOUPDATE = 10;
const URKEL_EBADWRITE = 11;
const URKEL_EBADOPEN = 12;
const URKEL_EITEREND = 13;

/**
 * Verification error codes.
 * @enum {Number}
 */

const codes = {
  URKEL_OK,
  URKEL_EHASHMISMATCH,
  URKEL_ESAMEKEY,
  URKEL_ESAMEPATH,
  URKEL_ENEGDEPTH,
  URKEL_EPATHMISMATCH,
  URKEL_ETOODEEP,
  URKEL_EINVAL,
  URKEL_ENOTFOUND,
  URKEL_ECORRUPTION,
  URKEL_ENOUPDATE,
  URKEL_EBADWRITE,
  URKEL_EBADOPEN,
  URKEL_EITEREND
};

const codesByVal = [
  'URKEL_OK',
  'URKEL_EHASHMISMATCH',
  'URKEL_ESAMEKEY',
  'URKEL_ESAMEPATH',
  'URKEL_ENEGDEPTH',
  'URKEL_EPATHMISMATCH',
  'URKEL_ETOODEEP',
  'URKEL_EINVAL',
  'URKEL_ENOTFOUND',
  'URKEL_ECORRUPTION',
  'URKEL_ENOUPDATE',
  'URKEL_EBADWRITE',
  'URKEL_EBADOPEN',
  'URKEL_EITEREND'
];

/*
 * Virtual TX Ops
 */

const VTX_OP_INSERT = 1;
const VTX_OP_REMOVE = 2;

class Tree {
  constructor(options) {
    this.options = new TreeOptions(options);
    this.prefix = this.options.prefix;

    this.isOpen = false;
    this.tree = null;

    this.init();
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

    return key.length === this.keySize;
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

    await nurkel.open(this.tree, this.prefix);
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
  }

  /**
   * Get current root node.
   * NOTE: This can only be achieved if we have another abstraction from the
   * NAPI for the Nodes. I don't think it's necessary. This method is here for
   * API compatibility purposes.
   * @throws {Error}
   */

  async getRoot() {
    throw new Error(ERR_PLACEHOLDER);
  }

  /**
   * Get root hash.
   * @returns {Buffer}
   */

  rootHash() {
    return nurkel.root_hash_sync(this.tree);
  }

  /**
   * Get root hash from the liburkel tree.
   * @returns {Buffer}
   */

  treeRootHashSync() {
    return nurkel.root_hash_sync(this.tree);
  }

  /**
   * Get root hash from the liburkel tree.
   * @returns {Promise<Buffer>}
   */

  async treeRootHash() {
    return nurkel.root_hash(this.tree);
  }

  /**
   * Get value by key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async get(key) {
    // TODO: Maybe remove exception handling
    // from this and instead handle this in nurkel.c.
    try {
      return await nurkel.get(this.tree, key);
    } catch (e) {
      if (e.code === codesByVal[URKEL_ENOTFOUND])
        return null;

      throw e;
    }
  }

  /**
   * Get value by key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  getSync(key) {
    // TODO: Maybe remove exception handling
    // from this and instead handle this in nurkel.c.
    try {
      return nurkel.get_sync(this.tree, key);
    } catch (e) {
      if (e.code === codesByVal[URKEL_ENOTFOUND])
        return null;

      throw e;
    }
  }

  /**
   * Does tree have key.
   * @param {Buffer} key
   * @returns {Promise<Boolean>}
   */

  async has(key) {
    return nurkel.has(this.tree, key);
  }

  /**
   * Does tree have key.
   * @param {Buffer} key
   * @returns {Boolean}
   */

  hasSync(key) {
    return nurkel.has_sync(this.tree, key);
  }

  /**
   * Inject new root into the tree.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async inject(hash) {
    return nurkel.inject(this.tree, hash);
  }

  /**
   * Inject new root into the tree.
   * @param {Hash} hash
   * @returns {void}
   */

  injectSync(hash) {
    return nurkel.inject_sync(this.tree, hash);
  }

  /**
   * Generate proof for the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async prove(key) {
    return nurkel.prove(this.tree, key);
  }

  /**
   * Generate proof for the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  proveSync(key) {
    return nurkel.prove_sync(this.tree, key);
  }

  /**
   * Compact database.
   * TODO: Implement this in liburkel.
   * @returns {Promise}
   */

  async compact() {
    throw new Error(ERR_NOT_IMPL);
  }

  /**
   * Compact database.
   * TODO: Implement this in liburkel.
   * @returns {Promise}
   */

  compactSync() {
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
    return new Snapshot(this, hash);
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

  /**
   * Destroy the database.
   * @param {String} path
   * @returns {Promise}
   */

  static async destroy(path) {
    return nurkel.destroy(path);
  }

  /**
   * Destroy the database.
   * @param {String} path
   * @returns {void}
   */

  static destroySync(path) {
    return nurkel.destroy_sync(path);
  }

  /**
   * Hash the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  static async hash(key) {
    return nurkel.hash(key);
  }

  /**
   * Hash the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  static hashSync(key) {
    return nurkel.hash_sync(key);
  }

  /**
   * Verify proof.
   * @param {Buffer} root
   * @param {Buffer} key
   * @param {Buffer} proof
   * @returns {Promise<[Number, Buffer?, Boolean]>}
   */

  static async verify(root, key, proof) {
    // TODO: Maybe move remove exception from here
    // and instead move to the nurkel.c.
    try {
      const [exists, value] = await nurkel.verify(root, key, proof);
      return [URKEL_OK, exists ? value : null];
    } catch (e) {
      if (codes[e.code])
        return [codes[e.code], null];

      throw e;
    }
  }

  /**
   * Verify proof.
   * @param {Buffer} root
   * @param {Buffer} key
   * @param {Buffer} proof
   * @returns {[Number, Buffer?, Boolean]} - exists
   */

  static verifySync(root, key, proof) {
    // TODO: Maybe move remove exception from here
    // and instead move to the nurkel.c.
    try {
      const [exists, value] = nurkel.verify_sync(root, key, proof);
      return [URKEL_OK, exists ? value : null];
    } catch (e) {
      if (codes[e.code])
        return [codes[e.code], null];

      throw e;
    }
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
   * Open transaction
   * @returns {Promise}
   */

  async open() {
    assert(!this.isOpen, ERR_TX_OPEN);
    await nurkel.tx_open(this.tx, this.hash);
    this.isOpen = true;

    if (!this.hash)
      this.hash = this.tree.rootHash();
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
    assert(this.isOpen, ERR_TX_NOT_OPEN);

    await nurkel.tx_close(this.tx);
    this.isOpen = false;
    this.tx = null;
    return;
  }

  /**
   * Get snapshot has.
   * @returns {Buffer}
   */

  rootHash() {
    return this.hash;
  }

  /**
   * Root Node wont be returned, here for API compatibility.
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
    return nurkel.tx_inject(this.tx, hash);
  }

  /**
   * Inject new root to the snapshot.
   * @param {Hash} hash
   * @returns {Promise}
   */

  injectSync(hash) {
    return nurkel.tx_inject_sync(this.tx, hash);
  }

  /**
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>} - value
   */

  async get(key) {
    // TODO: Maybe move remove exception from here
    // and instead move to the nurkel.c.
    try {
      return await nurkel.tx_get(this.tx, key);
    } catch (e) {
      if (e.code === codesByVal[URKEL_ENOTFOUND])
        return null;

      throw e;
    }
  }

  /**
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  getSync(key) {
    // TODO: Maybe move remove exception from here
    // and instead move to the nurkel.c.
    try {
      return nurkel.tx_get_sync(this.tx, key);
    } catch (e) {
      if (e.code === codesByVal[URKEL_ENOTFOUND])
        return null;

      throw e;
    }
  }

  /**
   * Does transaction have key (tree included)
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async has(key) {
    return nurkel.tx_has(this.tx, key);
  }

  /**
   * Does transaction have key (tree included)
   * @param {Buffer} key
   * @returns {Buffer}
   */

  hasSync(key) {
    return nurkel.tx_has_sync(this.tx, key);
  }

  /**
   * Get proof for the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async prove(key) {
    return nurkel.tx_prove(this.tx, key);
  }

  /**
   * Get proof for the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  proveSync(key) {
    return nurkel.tx_prove_sync(this.tx, key);
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

  /**
   * Calculate root hash of the transaction.
   * TODO: Benchmark this on the big transactions.
   * @returns {Buffer}
   */

  rootHash() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_root_hash_sync(this.tx);
  }

  /**
   * Get root hash from the liburkel tx.
   * @returns {Buffer}
   */

  txRootHashSync() {
    return nurkel.tx_root_hash_sync(this.tx);
  }

  /**
   * Get root hash from the liburkel tx.
   * @returns {Promise<Buffer>}
   */

  async txRootHash() {
    return nurkel.tx_root_hash(this.tx);
  }

  async getRoot() {
    throw new Error(ERR_NOT_IMPL);
  }

  /**
   * Insert key/val in the tx.
   * @param {Buffer} key
   * @param {Buffer} value
   * @returns {Promise}
   */

  async insert(key, value) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_insert(this.tx, key, value);
  }

  /**
   * Insert key/val in the tx.
   * @param {Buffer} key
   * @param {Buffer} value
   * @returns {void}
   */

  insertSync(key, value) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_insert_sync(this.tx, key, value);
  }

  /**
   * Remove entry from the tx.
   * @param {Buffer} key
   * @returns {Promise}
   */

  async remove(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_remove(this.tx, key);
  }

  /**
   * Remove entry from the tx.
   * @param {Buffer} key
   * @returns {void}
   */

  removeSync(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_remove_sync(this.tx, key);
  }

  async commit() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_commit(this.tx);
  }

  commitSync() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_commit_sync(this.tx);
  }

  async clear() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_clear(this.tx);
  }

  clearSync() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_clear_sync(this.tx);
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
exports.codes = codes;
exports.codesByVal = codesByVal;
