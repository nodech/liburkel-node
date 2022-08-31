/**
 * tree.js - JS Wrapper for the nurkel.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */
'use strict';

const assert = require('bsert');
const fs = require('bfile');
const nurkel = require('loady')('nurkel', __dirname);
const Proof = require('./proof');
const {randomPath} = require('./util');
const {
  asyncIterator,
  errors,
  statusCodes,
  statusCodesByVal
} = require('./common');

const {URKEL_OK, URKEL_ENOTFOUND} = statusCodes;

const {
  ERR_PLACEHOLDER,
  ERR_NOT_IMPL,
  ERR_NOT_SUPPORTED,

  ERR_INIT,
  ERR_NOT_INIT,
  ERR_OPEN,
  ERR_CLOSED,

  ERR_TX_OPEN,
  ERR_TX_NOT_OPEN,
  ERR_TX_NOT_FLUSHED
} = errors;

class Tree {
  constructor(options) {
    this.options = new TreeOptions(options);
    this.prefix = this.options.prefix;

    this.isOpen = false;
    this.tree = null;

    this.init();
  }

  get supportsSync() {
    return true;
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
   * @private
   * @returns Tree
   */

  init() {
    assert(!this.tree, ERR_INIT);

    this.tree = nurkel.init();
    return this;
  }

  /**
   * Open tree.
   * @param {Hash} [rootHash=null]
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

  getRoot() {
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
   * Get value by the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async get(key) {
    // TODO: Maybe remove exception handling
    // from this and instead handle this in nurkel.c.
    try {
      return await nurkel.get(this.tree, key);
    } catch (e) {
      if (e.code === statusCodesByVal[URKEL_ENOTFOUND])
        return null;

      throw e;
    }
  }

  /**
   * Get value by the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  getSync(key) {
    // TODO: Maybe remove exception handling
    // from this and instead handle this in nurkel.c.
    try {
      return nurkel.get_sync(this.tree, key);
    } catch (e) {
      if (e.code === statusCodesByVal[URKEL_ENOTFOUND])
        return null;

      throw e;
    }
  }

  /**
   * Does tree have the key.
   * @param {Buffer} key
   * @returns {Promise<Boolean>}
   */

  async has(key) {
    return nurkel.has(this.tree, key);
  }

  /**
   * Does tree have the key.
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
   * @returns {Promise<Proof>}
   */

  async prove(key) {
    const proof = new Proof();
    const raw = await nurkel.prove(this.tree, key);
    proof.decode(raw);
    return proof;
  }

  /**
   * Generate proof for the key.
   * @param {Buffer} key
   * @returns {Proof}
   */

  proveSync(key) {
    const proof = new Proof();
    const raw = nurkel.prove_sync(this.tree, key);
    proof.decode(raw);
    return proof;
  }

  /**
   * Verify proof.
   * @param {Buffer} root
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {Promise<[NurkelStatus, Buffer?]>}
   */

  async verify(root, key, proof) {
    return Tree.verify(root, key, proof);
  }

  /**
   * Verify proof.
   * @param {Buffer} root
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {[NurkelStatus, Buffer?]}
   */

  verifySync(root, key, proof) {
    return Tree.verifySync(root, key, proof);
  }

  /**
   * Get the tree stat.
   * @returns {Promise<Object>}
   */

  stat() {
    return Tree.stat(this.prefix);
  }

  /**
   * Get the tree stat.
   * @returns {Object}
   */

  statSync() {
    return Tree.statSync(this.prefix);
  }

  /**
   * Compact database.
   * @param {String} [tmpPrefix]
   * @param {Buffer} [root=null]
   * @returns {Promise}
   */

  async compact(tmpPrefix, root) {
    if (this.isOpen)
      await this.close();

    await Tree.compact(this.prefix, tmpPrefix, root);
    await this.open();
  }

  /**
   * Compact database.
   * NOTE: Sync version will not attempt to close,
   * instead will hang if the tree is open.
   * @param {String} [tmpPrefix]
   * @param {Buffer} [root=null]
   * @returns {Promise}
   */

  compactSync(tmpPrefix, root) {
    assert(!this.isOpen, 'Can not compact when db is open.');

    Tree.compactSync(this.prefix, tmpPrefix, root);
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
   * @returns {Promise<[NurkelStatus, Buffer?]>}
   */

  static async verify(root, key, proof) {
    assert(proof instanceof Proof);
    // TODO: Maybe move remove exception from here
    // and instead move to the nurkel.c.
    try {
      const [exists, value] = await nurkel.verify(root, key, proof.raw);
      return [URKEL_OK, exists ? value : null];
    } catch (e) {
      if (statusCodes[e.code])
        return [statusCodes[e.code], null];

      throw e;
    }
  }

  /**
   * Verify proof.
   * @param {Buffer} root
   * @param {Buffer} key
   * @param {Buffer} proof
   * @returns {[NurkelStatus, Buffer?]} - exists
   */

  static verifySync(root, key, proof) {
    assert(proof instanceof Proof);
    // TODO: Maybe move remove exception from here
    // and instead move to the nurkel.c.
    try {
      const [exists, value] = nurkel.verify_sync(root, key, proof.raw);
      return [URKEL_OK, exists ? value : null];
    } catch (e) {
      if (statusCodes[e.code])
        return [statusCodes[e.code], null];

      throw e;
    }
  }

  /**
   * Compact the tree.
   * @param {String} path
   * @param {String} [tmpPrefix]
   * @param {Buffer?} [root=null]
   * @returns {Promise}
   */

  static async compact(path, tmpPrefix, root) {
    if (!tmpPrefix)
      tmpPrefix = randomPath(path);

    await nurkel.compact(path, tmpPrefix, root);
    await Tree.destroy(path);
    await fs.rename(tmpPrefix, path);
  }

  /**
   * Compact the tree.
   * @param {String} path
   * @param {String} [tmpPrefix]
   * @param {Buffer} [root=null]
   * @returns {void}
   */

  static compactSync(path, tmpPrefix, root) {
    if (!tmpPrefix)
      tmpPrefix = randomPath(path);

    nurkel.compact_sync(path, tmpPrefix, root);
    Tree.destroySync(path);
    fs.renameSync(tmpPrefix, path);
  }

  /**
   * Get tree stats.
   * @param {String} prefix
   * @returns {Promise<Object>}
   */

  static stat(prefix) {
    return nurkel.stat(prefix);
  }

  /**
   * Get tree stats.
   * @param {String} prefix
   * @returns {Object}
   */

  static statSync(prefix) {
    return nurkel.stat_sync(prefix);
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
  constructor(tree, rootHash) {
    this.tree = tree;
    this.hash = rootHash;

    this.initialized = false;
    this.isOpen = false;
    this.tx = null;

    this.init();
  }

  /**
   * Initialize snapshot
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
   * @param {Hash} rootHash
   * @returns {Promise}
   */

  async inject(rootHash) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    await nurkel.tx_inject(this.tx, rootHash);
    this.hash = rootHash;
  }

  /**
   * Inject new root to the snapshot.
   * @param {Hash} rootHash
   * @returns {Promise}
   */

  injectSync(rootHash) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    nurkel.tx_inject_sync(this.tx, rootHash);
    this.hash = rootHash;
  }

  /**
   * Verify proof.
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {Promise<[NurkelStatus, Buffer?]>}
   */

  async verify(key, proof) {
    return Tree.verify(this.rootHash(), key, proof);
  }

  /**
   * Verify proof.
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {[NurkelStatus, Buffer?]} - exists
   */

  verifySync(key, proof) {
    return Tree.verifySync(this.rootHash(), key, proof);
  }

  /**
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>} - value
   */

  async get(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    // TODO: Maybe move remove exception from here
    // and instead move to the nurkel.c.
    try {
      return await nurkel.tx_get(this.tx, key);
    } catch (e) {
      if (e.code === statusCodesByVal[URKEL_ENOTFOUND])
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
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    // TODO: Maybe move remove exception from here
    // and instead move to the nurkel.c.
    try {
      return nurkel.tx_get_sync(this.tx, key);
    } catch (e) {
      if (e.code === statusCodesByVal[URKEL_ENOTFOUND])
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
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_has(this.tx, key);
  }

  /**
   * Does transaction have key (tree included)
   * @param {Buffer} key
   * @returns {Buffer}
   */

  hasSync(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_has_sync(this.tx, key);
  }

  /**
   * Get proof for the key.
   * @param {Buffer} key
   * @returns {Promise<Proof>}
   */

  async prove(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    const proof = new Proof();
    const raw = await nurkel.tx_prove(this.tx, key);
    proof.decode(raw);
    return proof;
  }

  /**
   * Get proof for the key.
   * @param {Buffer} key
   * @returns {Proof}
   */

  proveSync(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    const proof = new Proof();
    const raw = nurkel.tx_prove_sync(this.tx, key);
    proof.decode(raw);
    return proof;
  }

  iterator(read = true) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
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
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_root_hash_sync(this.tx);
  }

  /**
   * Get root hash from the liburkel tx.
   * @returns {Promise<Buffer>}
   */

  async txRootHash() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
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

  /**
   * Commit transaction.
   * @returns {Promise<Hash>}
   */

  async commit() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_commit(this.tx);
  }

  /**
   * Commit transaction.
   * @returns {Hash}
   */

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

Tree.supportsSync = true;
exports.Tree = Tree;
exports.statusCodes = statusCodes;
exports.statusCodesByVal = statusCodesByVal;;
