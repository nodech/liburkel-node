/**
 * urkel.js - urkel wrapper in nurkel fashion.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */
'use strict';

const assert = require('bsert');
const urkel = require('urkel');
const {Tree} = urkel;
const UrkelProof = urkel.Proof;
const Proof = require('./proof');

const {
  asyncIterator,
  syncIterator,
  errors,
  statusCodes,
  statusCodesByVal,
  iteratorTypes
} = require('./common');

const {randomPath} = require('./util');
const BLAKE2b = require('./blake2b');

const {
  ERR_PLACEHOLDER,
  ERR_NOT_SUPPORTED,
  ERR_NOT_IMPL,

  ERR_OPEN,
  ERR_NOT_OPEN,
  ERR_CLOSED,

  ERR_TX_OPEN,
  ERR_TX_NOT_OPEN
} = errors;

const {
  ITER_TYPE_KEYS,
  ITER_TYPE_VALUES,
  ITER_TYPE_KEY_VAL
} = iteratorTypes;

const handleMissingError = (e) => {
  if (e.code === 'ERR_MISSING_NODE')
    e.code = statusCodesByVal[statusCodes.URKEL_ENOTFOUND];

  return e;
};

/**
 * Wrapper for the urkel Tree.
 */

class WrappedTree {
  constructor(options = {}) {
    options.hash = BLAKE2b;
    options.bits = BLAKE2b.bits;
    this._tree = new Tree(options);
    this.isOpen = false;
  }

  get supportsSync() {
    return false;
  }

  get bits() {
    return this._tree.bits;
  }

  get hashSize() {
    return this._tree.hash.size;
  }

  get keySize() {
    return this.bits >>> 3;
  }

  get hash() {
    return this._tree.hash;
  }

  /**
   * Check hash.
   * @param {Buffer} hash
   * @returns {Boolean}
   */

  isHash(hash) {
    return this._tree.isHash(hash);
  }

  /**
   * Check key.
   * @param {Buffer} key
   * @returns {Boolean}
   */

  isKey(key) {
    return this._tree.isKey(key);
  }

  /**
   * Check value.
   * @param {Buffer} value
   * @returns {Boolean}
   */

  isValue(value) {
    return this._tree.isValue(value);
  }

  /**
   * Open tree.
   * @param {Hash} [rootHash=null]
   * @returns {Promise}
   */

  async open(rootHash) {
    assert(!this.isOpen, ERR_OPEN);

    try {
      await this._tree.open(rootHash);
    } catch (e) {
      throw handleMissingError(e);
    }
    this.isOpen = true;
  }

  /**
   * Close tree.
   * @returns {Promise}
   */

  async close() {
    assert(this.isOpen, ERR_CLOSED);
    await this._tree.close();
    this.isOpen = false;
  }

  /**
   * Removed from the nurkel, so even memory wont
   * support it.
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
    return this._tree.rootHash();
  }

  /**
   * Get root hash sync.
   * @returns {Buffer}
   */

  treeRootHashSync() {
    assert(this.isOpen, ERR_NOT_OPEN);
    return this._tree.rootHash();
  }

  /**
   * Get root hash but wrap in the promise.
   * @returns {Promise<Buffer>}
   */

  async treeRootHash() {
    assert(this.isOpen, ERR_NOT_OPEN);
    return this._tree.rootHash();
  }

  /**
   * Get value by the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async get(key) {
    assert(this.isOpen, ERR_NOT_OPEN);
    const snap = this._tree.snapshot();
    const value = await snap.get(key);

    return value;
  }

  /**
   * Get value by the key.
   */

  getSync() {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Does tree have the key.
   * @param {Buffer} key
   * @returns {Promise<Boolean>}
   */

  async has(key) {
    assert(this.isOpen, ERR_NOT_OPEN);
    const snap = this._tree.snapshot();
    const value = await snap.get(key);

    return value != null;
  }

  /**
   * Does tree have the key.
   */

  hasSync(key) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Inject new root into the tree.
   * @param {Hash} root
   * @returns {Promise}
   */

  async inject(root) {
    assert(this.isOpen, ERR_NOT_OPEN);
    try {
      return await this._tree.inject(root);
    } catch (e) {
      throw handleMissingError(e);
    }
  }

  /**
   * Inject new root into the tree.
   */

  injectSync(root) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Generate proof for the key.
   * @param {Buffer} key
   * @returns {Promise<Proof>}
   */

  async prove(key) {
    assert(this.isOpen, ERR_NOT_OPEN);
    const uproof = await this._tree.prove(key);
    const raw = uproof.encode(this.hash, this.bits);
    return Proof.decode(raw);
  }

  /**
   * Generate proof for the key.
   */

  proveSync(key) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Get the tree stat.
   * @returns {Promise<Object>}
   */

  stat() {
    return this._tree.store.stat();
  }

  /**
   * Get the tree stat.
   * @returns {Object}
   */

  statSync() {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Verify proof.
   * @param {Buffer} root
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {Promise<[NurkelStatus, Buffer?]>}
   */

  async verify(root, key, proof) {
    return WrappedTree.verify(root, key, proof);
  }

  /**
   * Verify proof.
   * @param {Buffer} root
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {[NurkelStatus, Buffer?]}
   */

  verifySync(root, key, proof) {
    return WrappedTree.verifySync(root, key, proof);
  }

  /**
   * Compact database.
   * @param {String} [tmpPrefix]
   * @param {Buffer} [root=null]
   * @returns {Promise}
   */

  async compact(tmpPrefix, root) {
    if (root)
      await this.inject(root);

    await this._tree.compact(tmpPrefix);
  }

  /**
   * Compact database.
   * @param {String} [tmpPrefix]
   * @param {Buffer} [root=null]
   * @returns {Promise}
   */

  compactSync(tmpPrefix, root) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Get new transaction instance
   * @returns {Transaction}
   */

  transaction() {
    return new WrappedTransaction(this);
  }

  /**
   * Get snapshot
   * @param {Hash} rootHash
   * @returns {Snapshot}
   */

  snapshot(rootHash) {
    return new WrappedSnapshot(this, rootHash);
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
   * Compact
   */

  vtransaction() {
    return this.transaction();
  }

  vtxn() {
    return this.transaction();
  }

  vbatch() {
    return this.transaction();
  }

  /**
   * Destroy the database.
   * @param {String} path
   * @returns {Promise}
   */

  static async destroy(path) {
    const tree = new WrappedTree({ prefix: path });
    await tree._tree.store.destroy();
    return;
  }

  /**
   * Destroy the database.
   * @param {String} path
   * @returns {void}
   */

  static destroySync(path) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Hash the key.
   * @param {Buffer} data
   * @returns {Promise<Buffer>}
   */

  static async hash(data) {
    return BLAKE2b.digest(data);
  }

  /**
   * Hash the key.
   * @param {Buffer} data
   * @returns {Buffer}
   */

  static hashSync(data) {
    return BLAKE2b.digest(data);
  }

  /**
   * Verify proof.
   * @param {Buffer} root
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {Promise<[NurkelStatus, Buffer?]>}
   */

  static async verify(root, key, proof) {
    const proofobj = UrkelProof.decode(proof.raw, BLAKE2b, BLAKE2b.bits);
    const [code, value] = proofobj.verify(root, key, BLAKE2b, BLAKE2b.bits);
    return [code, value];
  }

  /**
   * Verify proof.
   * @param {Buffer} root
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {[NurkelStatus, Buffer?]}
   */

  static verifySync(root, key, proof) {
    const proofobj = UrkelProof.decode(proof.raw, BLAKE2b, BLAKE2b.bits);
    const [code, value] = proofobj.verify(root, key, BLAKE2b, BLAKE2b.bits);
    return [code, value];
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

    const tree = new WrappedTree({ prefix: path });
    await tree.open();
    await tree.compact(tmpPrefix, root);
    await tree.close();
  }

  /**
   * Compact the tree.
   * @param {String} path
   * @param {String} [tmpPrefix]
   * @param {Buffer} [root=null]
   * @returns {void}
   */

  static compactSync(path, tmpPrefix, root) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Get tree stats.
   * @param {String} prefix
   * @returns {Promise<Object>}
   */

  static async stat(prefix) {
    const tree = new WrappedTree({ prefix });
    await tree.open();
    const res = await tree.stat();
    await tree.close();
    return res;
  }

  /**
   * Get tree stats.
   * @param {String} prefix
   * @returns {Object}
   */

  static statSync(prefix) {
    throw new Error(ERR_NOT_SUPPORTED);
  }
}

class WrappedSnapshot {
  /**
   * @param {WrappedTree} wtree
   * @param {Hash} [rootHash=null]
   */

  constructor(wtree, rootHash) {
    this.tree = wtree;
    this._tx = null;
    this.isOpen = false;
    this.initialized = false;

    this.init(rootHash);
  }

  get hash() {
    return this._tx.rootHash();
  }

  /**
   * Initialize snapshot
   */

  init(rootHash) {
    assert(this.initialized === false);
    this.initialized = true;
    try {
      this._tx = this.tree._tree.snapshot(rootHash);
    } catch (e) {
      throw handleMissingError(e);
    }
  }

  /**
   * Open transaction
   * @returns {Promise}
   */

  async open() {
    assert(!this.isOpen, ERR_TX_OPEN);
    this.isOpen = true;
  }

  /**
   * Close the snapshot
   * @returns {Promise}
   */

  async close() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    this.isOpen = false;
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
    try {
      return await this._tx.inject(rootHash);
    } catch (e) {
      throw handleMissingError(e);
    }
  }

  /**
   * Inject new root to the snapshot.
   * @param {Hash} hash
   * @returns {Promise}
   */

  injectSync(hash) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Promise<Buffer>} - value
   */

  async get(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return this._tx.get(key);
  }

  /**
   * Returns value for the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  getSync(key) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Does transaction have key (tree included)
   * @param {Buffer} key
   * @returns {Promise<Buffer>}
   */

  async has(key) {
    const val = await this.get(key);
    return val != null;
  }

  /**
   * Does transaction have key (tree included)
   * @param {Buffer} key
   * @returns {Buffer}
   */

  hasSync(key) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Get proof for the key.
   * @param {Buffer} key
   * @returns {Promise<Proof>}
   */

  async prove(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    let uproof;
    try {
      uproof = await this._tx.prove(key);
    } catch (e) {
      throw handleMissingError(e);
    }
    const raw = uproof.encode(this.tree.hash, this.tree.bits);
    return Proof.decode(raw);
  }

  /**
   * Get proof for the key.
   * @param {Buffer} key
   * @returns {Buffer}
   */

  proveSync(key) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Verify proof.
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {Promise<[NurkelStatus, Buffer?]>}
   */

  async verify(key, proof) {
    return WrappedTree.verify(this.rootHash(), key, proof);
  }

  /**
   * Verify proof.
   * @param {Buffer} key
   * @param {Proof} proof
   * @returns {[NurkelStatus, Buffer?]} - exists
   */

  verifySync(key, proof) {
    return WrappedTree.verifySync(this.rootHash(), key, proof);
  }

  iterator(cacheSize = 1) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    const iter = new Iterator(this, cacheSize);
    iter.root = this.hash;
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

class WrappedTransaction extends WrappedSnapshot {
  /**
   * @param {WrappedTree} wtree
   */

  constructor(wtree) {
    super(wtree, wtree.rootHash());
  }

  init() {
    this._tx = this.tree._tree.txn();
  }

  /**
   * Calculate root hash of the transaction.
   * TODO: Benchmark this on the big transactions.
   * @returns {Buffer}
   */

  rootHash() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return this._tx.rootHash();
  }

  /**
   * Get root hash from the liburkel tx.
   * @returns {Buffer}
   */

  txRootHashSync() {
    return this._tx.rootHash();
  }

  /**
   * Get root hash from the liburkel tx.
   * @returns {Promise<Buffer>}
   */

  async txRootHash() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return this._tx.rootHash();
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
    return this._tx.insert(key, value);
  }

  /**
   * Insert key/val in the tx.
   * @param {Buffer} key
   * @param {Buffer} value
   * @returns {void}
   */

  insertSync(key, value) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Remove entry from the tx.
   * @param {Buffer} key
   * @returns {Promise}
   */

  async remove(key) {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return this._tx.remove(key);
  }

  /**
   * Remove entry from the tx.
   * @param {Buffer} key
   * @returns {void}
   */

  removeSync(key) {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  /**
   * Commit transaction.
   * @returns {Promise<Hash>}
   */

  async commit() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return this._tx.commit();
  }

  /**
   * Commit transaction.
   */

  commitSync() {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  async clear() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return this._tx.clear();
  }

  clearSync() {
    assert(this.isOpen, ERR_TX_NOT_OPEN);
    return this._tx.clear();
  }
}

class Iterator {
  /**
   * @param {WrappedSnapshot|WrappedTransaction} tx
   * @param {Number} [cacheSize = 1] - does not affect urkel.
   */

  constructor(tx, cacheSize) {
    this.tx = tx;
    this.root = null;
    this.iter = null;
    this.init();
  }

  init() {
    assert(this.tx.isOpen, ERR_TX_NOT_OPEN);
    this.iter = this.tx._tx.iterator(true);
  }

  async _next() {
    const next = await this.iter.next();

    if (!next)
      return null;

    return {
      key: this.iter.key,
      value: this.iter.value
    };
  }

  async end() {
    return Promise.resolve();
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
    throw new Error(ERR_NOT_SUPPORTED);
  }

  valueSync() {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  entriesSync() {
    throw new Error(ERR_NOT_SUPPORTED);
  }

  [syncIterator]() {
    return this.entriesSync();
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

WrappedTree.supportsSync = false;
exports.Tree = WrappedTree;
exports.Transaction = WrappedTransaction;
exports.Snapshot = WrappedSnapshot;
exports.VirtualTransaction = WrappedTransaction;
exports.statusCodes = statusCodes;
exports.statusCodesByVal = statusCodesByVal;
