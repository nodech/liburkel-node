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
const {BufferMap} = require('buffer-map');
const {
  asyncIterator,
  syncIterator,
  errors,
  statusCodes,
  statusCodesByVal,
  iteratorTypes
} = require('./common');

const {
  ERR_PLACEHOLDER,
  ERR_NOT_IMPL,
  ERR_NOT_SUPPORTED,

  ERR_INIT,
  ERR_NOT_INIT,
  ERR_OPEN,
  ERR_NOT_OPEN,
  ERR_CLOSED,

  ERR_TX_OPEN,
  ERR_TX_NOT_OPEN,
  ERR_TX_NOT_FLUSHED
} = errors;

const {
  ITER_TYPE_KEYS,
  ITER_TYPE_VALUES,
  ITER_TYPE_KEY_VAL
} = iteratorTypes;

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

    this.tree = nurkel.tree_init();
    return this;
  }

  /**
   * Open tree.
   * @param {Hash} [rootHash=null]
   * @returns {Promise}
   */

  async open(rootHash) {
    assert(!this.isOpen, ERR_OPEN);

    await nurkel.tree_open(this.tree, this.prefix);
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

    await nurkel.tree_close(this.tree);
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
    assert(this.isOpen, ERR_NOT_OPEN);
    return nurkel.tree_root_hash_sync(this.tree);
  }

  /**
   * Get root hash from the liburkel tree.
   * @returns {Buffer}
   */

  treeRootHashSync() {
    assert(this.isOpen, ERR_NOT_OPEN);
    return nurkel.tree_root_hash_sync(this.tree);
  }

  /**
   * Get root hash from the liburkel tree.
   * @returns {Promise<Buffer>}
   */

  async treeRootHash() {
    assert(this.isOpen, ERR_NOT_OPEN);
    return nurkel.tree_root_hash(this.tree);
  }

  /**
   * Get value by the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async get(key) {
    assert(this.isOpen, ERR_NOT_OPEN);
    return nurkel.tree_get(this.tree, key);
  }

  /**
   * Get value by the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  getSync(key) {
    assert(this.isOpen, ERR_NOT_OPEN);
    return nurkel.tree_get_sync(this.tree, key);
  }

  /**
   * Does tree have the key.
   * @param {Buffer} key
   * @returns {Promise<Boolean>}
   */

  async has(key) {
    assert(this.isOpen, ERR_NOT_OPEN);
    return nurkel.tree_has(this.tree, key);
  }

  /**
   * Does tree have the key.
   * @param {Buffer} key
   * @returns {Boolean}
   */

  hasSync(key) {
    assert(this.isOpen, ERR_NOT_OPEN);
    return nurkel.tree_has_sync(this.tree, key);
  }

  /**
   * Inject new root into the tree.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async inject(hash) {
    assert(this.isOpen, ERR_NOT_OPEN);
    return nurkel.tree_inject(this.tree, hash);
  }

  /**
   * Inject new root into the tree.
   * @param {Hash} hash
   * @returns {void}
   */

  injectSync(hash) {
    assert(this.isOpen, ERR_NOT_OPEN);
    return nurkel.tree_inject_sync(this.tree, hash);
  }

  /**
   * Generate proof for the key.
   * @param {Buffer} key
   * @returns {Promise<Proof>}
   */

  async prove(key) {
    assert(this.isOpen, ERR_NOT_OPEN);
    const proof = new Proof();
    const raw = await nurkel.tree_prove(this.tree, key);
    proof.decode(raw);
    return proof;
  }

  /**
   * Generate proof for the key.
   * @param {Buffer} key
   * @returns {Proof}
   */

  proveSync(key) {
    assert(this.isOpen, ERR_NOT_OPEN);
    const proof = new Proof();
    const raw = nurkel.tree_prove_sync(this.tree, key);
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
    assert(this.isOpen, ERR_NOT_OPEN);
    return new Transaction(this);
  }

  /**
   * Get snapshot
   * @param {Hash} hash
   * @returns {Snapshot}
   */

  snapshot(hash) {
    assert(this.isOpen, ERR_NOT_OPEN);
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
   * Get new transaction instance
   * @returns {VirtualTransaction}
   */

  vtransaction() {
    return new VirtualTransaction(this);
  }

  /**
   * Get new virtual transaction instance
   * @returns {VirtualTransaction}
   */

  vtxn() {
    return this.vtransaction();
  }

  /**
   * Get new transaction instance
   * @returns {VirtualTransaction}
   */

  vbatch() {
    return this.vtransaction();
  }

  /**
   * Get debug info
   * @param {Boolean} showTransactions
   * @returns {Object}
   */

  debugInfoSync(showTransactions = false, showIterators = false) {
    assert(this.tree != null);
    const states = [
      'closed',
      'opening',
      'open',
      'closing'
    ];

    const obj = nurkel.tree_debug_info_sync(
      this.tree,
      showTransactions,
      showIterators
    );

    obj.state = states[obj.state];

    if (!obj.transactions)
      return obj;

    for (const tx of obj.transactions) {
      tx.state = states[tx.state];

      if (!tx.iterators)
        continue;

      for (const iter of tx.iterators)
        iter.state = states[iter.state];
    }

    return obj;
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
    return nurkel.verify(root, key, proof.raw);
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
    return nurkel.verify_sync(root, key, proof.raw);
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
    this.tx = null;

    this.init();
  }

  /**
   * Initialize snapshot
   * @private
   */

  init() {
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
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>} - value
   */

  async get(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return await nurkel.tx_get(this.tx, key);
  }

  /**
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  getSync(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return nurkel.tx_get_sync(this.tx, key);
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
   * @param {Number} [cacheSize = 100] - Cache size for bulk reads.
   */

  iterator(cacheSize = 100) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    const iter = new Iterator(this, cacheSize);
    iter.root = this.root;
    return iter;
  }

  [asyncIterator]() {
    return this.entries();
  }

  keys() {
    const iter = this.iterator();
    return iter.keys();
  }

  values() {
    const iter = this.iterator();
    return iter.values();
  }

  entries() {
    const iter = this.iterator();
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

class VirtualTransaction {
  constructor(tree) {
    assert(tree instanceof Tree);

    this.tree = tree;
    this.hash = tree.rootHash();
    this.isOpen = false;

    this.tx = null;
    this.cached = new BufferMap();
    this.ops = [];

    this.init();
  }

  get isFlushed() {
    return this.ops.length === 0;
  }

  /**
   * Initialize nurkel TX.
   * @private
   */

  init() {
    this.tx = nurkel.tx_init(this.tree.tree);
  }

  /**
   * Open tx.
   * @returns {Promise}
   */

  async open() {
    assert(!this.isOpen, ERR_TX_OPEN);
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
    return;
  }

  /**
   * Flush
   * @returns {Boolean}
   */

  flushSync() {
    if (this.isFlushed)
      return false;

    nurkel.tx_apply_sync(this.tx, this.ops);
    this.cached.clear();
    this.ops.length = 0;

    return true;
  }

  /**
   * Maybe flush
   * @returns {Boolean}
   */

  maybeFlushSync() {
    if (!this.isFlushed)
      return this.flushSync();

    return false;
  }

  /**
   * Flush
   * @returns {Promise<Boolean>}
   */

  async flush() {
    if (this.isFlushed)
      return false;

    await nurkel.tx_apply(this.tx, this.ops);
    this.cached.clear();
    this.ops.length = 0;

    return true;
  }

  /**
   * Maybe flush
   * @returns {Promise<Boolean>}
   */

  async maybeFlush() {
    if (!this.isFlushed)
      return await this.flush();
    return false;
  }

  /**
   * This becomes almost UNSUPPORTED, because we don't want
   * to do anything heavy in sync ops. Instead use async version.
   * @returns {Buffer}
   */

  rootHash() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    assert(this.isFlushed, ERR_TX_NOT_FLUSHED);

    return nurkel.tx_root_hash_sync(this.tx);
  }

  /**
   * Get root hash from the liburkel tx.
   * Flush if necessary.
   * @returns {Promise}
   */

  async txRootHash() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);

    await this.maybeFlush();
    return nurkel.tx_root_hash(this.tx);
  }

  /**
   * Get root hash from the liburkel tx.
   * Flush if necessary.
   * @returns {Promise}
   */

  txRootHashSync() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);

    this.maybeFlushSync();
    return nurkel.tx_root_hash_sync(this.tx);
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
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    await this.maybeFlush();
    return nurkel.tx_inject(this.tx, hash);
  }

  /**
   * Inject new root to the snapshot.
   * @param {Hash} hash
   * @returns {void}
   */

  injectSync(hash) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    this.maybeFlushSync();
    return nurkel.tx_inject_sync(this.tx, hash);
  }

  /**
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>} - value
   */

  async get(key) {
    if (this.cached.has(key))
      return this.cached.get(key);

    // We need to flush if there are deletes in the OP list
    // that have not been applied to the tree.
    await this.maybeFlush();
    return nurkel.tx_get(this.tx, key);
  }

  /**
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  getSync(key) {
    if (this.cached.has(key))
      return this.cached.get(key);

    // We need to flush if there are deletes in the OP list
    // that have not been applied to the tree.
    this.maybeFlushSync();
    return nurkel.tx_get_sync(this.tx, key);
  }

  /**
   * Does transaction have key (tree included)
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async has(key) {
    if (this.cached.has(key))
      return true;

    await this.maybeFlush();
    return nurkel.tx_has(this.tx, key);
  }

  /**
   * Does transaction have key (tree included)
   * @param {Buffer} key
   * @returns {Buffer}
   */

  hasSync(key) {
    if (this.cached.has(key))
      return true;

    this.maybeFlushSync();
    return nurkel.tx_has_sync(this.tx, key);
  }

  /**
   * Get proof for the key.
   * @param {Buffer} key
   * @returns {Promise<Proof>}
   */

  async prove(key) {
    await this.maybeFlush();

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
    this.maybeFlushSync();

    const proof = new Proof();
    const raw = nurkel.tx_prove_sync(this.tx, key);
    proof.decode(raw);
    return proof;
  }

  /**
   * Insert key/val in the tx.
   * @param {Buffer} key
   * @param {Buffer} value
   * @returns {Promise}
   */

  async insert(key, value) {
    return this.insertSync(key, value);
  }

  /**
   * Insert key/val in the tx.
   * @param {Buffer} key
   * @param {Buffer} value
   * @returns {Promise}
   */

  insertSync(key, value) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);

    this.cached.set(key, value);
    this.ops.push([VTX_OP_INSERT, key, value]);
  }

  /**
   * Remove entry from the tx.
   * @param {Buffer} key
   * @returns {Promise}
   */

  async remove(key) {
    return this.removeSync(key);
  }

  /**
   * Remove entry from the tx.
   * @param {Buffer} key
   * @returns {Promise}
   */

  removeSync(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);

    if (this.cached.has(key))
      this.cached.delete(key);

    this.ops.push([VTX_OP_REMOVE, key]);
  }

  /**
   * Commit transaction to the tree.
   * @returns {Promise}
   */

  async commit() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    await this.maybeFlush();
    return nurkel.tx_commit(this.tx);
  }

  /**
   * Commit transaction to the tree.
   * @returns {void}
   */

  commitSync() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    this.maybeFlushSync();
    return nurkel.tx_commit(this.tx);
  }

  async clear() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    this.ops.length = 0;
    this.cached.clear();
    return nurkel.tx_clear(this.tx);
  }

  clearSync() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    this.ops.length = 0;
    this.cached.clear();
    return nurkel.tx_clear_sync(this.tx);
  }

  iterator() {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  [asyncIterator]() {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  keys() {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  values() {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  entries() {
    throw new Error(ERR_NOT_SUPPORTED);
  }
}

class Iterator {
  /**
   * @param {Snapshot|Transaction} tx
   * @param {Number} [cacheSize=100] - Number of elements to read at once.
   */

  constructor(tx, cacheSize = 100) {
    assert(tx.tx != null);
    assert(typeof cacheSize === 'number');
    assert((cacheSize >>> 0) === cacheSize);
    assert(cacheSize > 0);

    this.tx = tx;
    this.cacheSize = cacheSize;
    this.cache = [];
    this.cacheIndex = 0;
    this.iter = null;
    this.root = null;
    this.init();
  }

  /**
   * Init iterator.
   */

  init() {
    this.iter = nurkel.iter_init(this.tx.tx, this.cacheSize);
  }

  _nextSync() {
    if (this.cache.length === this.cacheIndex) {
      this.cache = nurkel.iter_next_sync(this.iter);
      this.cacheIndex = 0;

      if (this.cache.length === 0)
        return null;
    }

    return this.cache[this.cacheIndex++];
  }

  async _next() {
    if (this.cache.length === this.cacheIndex) {
      this.cache = await nurkel.iter_next(this.iter);
      this.cacheIndex = 0;

      if (this.cache.length === 0)
        return null;
    }

    return this.cache[this.cacheIndex++];
  }

  async end() {
    return nurkel.iter_close(this.iter);
  }

  keys() {
    return new AsyncIterator(this, ITER_TYPE_KEYS);
  }

  values() {
    return new AsyncIterator(this, ITER_TYPE_VALUES);
  }

  entries() {
    return new AsyncIterator(this, ITER_TYPE_KEY_VAL);
  }

  [asyncIterator]() {
    return this.entries();
  }

  keysSync() {
    return new SyncIterator(this, ITER_TYPE_KEYS);
  }

  valueSync() {
    return new SyncIterator(this, ITER_TYPE_VALUES);
  }

  entriesSync() {
    return new SyncIterator(this, ITER_TYPE_KEY_VAL);
  }

  [syncIterator]() {
    return this.entriesSync();
  }
}

class SyncIterator {
  constructor(iter, type) {
    this.iter = iter;
    this.type = type;
    this.root = null;
  }

  return(value) {
    const result = {
      value: value ? value : undefined,
      done: true
    };

    // Swallow if there are any errors because sync.
    this.iter.end().catch(() => {});
    return result;
  }

  throw(exception) {
    // Swallow if there are any errors because sync.
    this.iter.end().catch(() => {});

    if (exception)
      throw exception;

    throw undefined;
  }

  next() {
    const result = this.iter._nextSync();

    if (result == null)
      return { value: undefined, done: true };

    switch (this.type) {
      case ITER_TYPE_KEYS:
        return { value: result.key, done: false };
      case ITER_TYPE_VALUES:
        return { value: result.value, done: false };
      case ITER_TYPE_KEY_VAL:
        return { value: [result.key, result.value], done: false };
      default:
        throw new Error('Bad value mode.');
    }
  }
}

class AsyncIterator {
  constructor(iter, type) {
    this.iter = iter;
    this.type = type;
  }

  async next() {
    const result = await this.iter._next();

    if (result == null)
      return { value: undefined, done: true };

    switch (this.type) {
      case ITER_TYPE_KEYS:
        return { value: result.key, done: false };
      case ITER_TYPE_VALUES:
        return { value: result.value, done: false };
      case ITER_TYPE_KEY_VAL:
        return { value: [result.key, result.value], done: false };
      default:
        throw new Error('Bad value mode.');
    }
  }

  async return(value) {
    const result = {
      value: value ? value : undefined,
      done: true
    };

    await this.iter.end();
    return result;
  }

  async throw(exception) {
    await this.iter.end();

    if (exception)
      return Promise.reject(exception);

    return Promise.reject(undefined);
  }
}

Tree.supportsSync = true;
exports.Tree = Tree;
exports.Transaction = Transaction;
exports.Snapshot = Snapshot;
exports.VirtualTransaction = VirtualTransaction;
exports.statusCodes = statusCodes;
exports.statusCodesByVal = statusCodesByVal;;
