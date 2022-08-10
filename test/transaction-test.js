'use strict';

const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir, randomKey} = require('./util/common');
const {Tree} = require('../lib/tree');

const NULL_HASH = Buffer.alloc(32, 0);

describe('Urkel Transaction', function () {
  if (!global.gc)
    this.skip();

  let prefix, tree;

  beforeEach(async () => {
    prefix = testdir('tx');
    fs.mkdirSync(prefix);

    tree = new Tree({prefix});
    await tree.open();
  });

  afterEach(async () => {
    await tree.close();

    if (fs.existsSync(prefix))
      rmTreeDir(prefix);
  });

  it('should get the root', async () => {
    const txn1 = tree.txn();
    await txn1.maybeOpen();

    assert.bufferEqual(txn1.txRootHashSync(), NULL_HASH);
    assert.bufferEqual(await txn1.txRootHash(), NULL_HASH);
  });

  it('should insert and get the key (sync)', async () => {
    const txn1 = tree.txn();
    await txn1.open();

    const key1 = randomKey();
    const value = Buffer.from('Hello !');

    assert.strictEqual(txn1.hasSync(key1), false);
    txn1.insertSync(key1, value);

    assert.strictEqual(txn1.hasSync(key1), true);
    const resValue = txn1.getSync(key1);
    assert.bufferEqual(resValue, value);
    txn1.removeSync(key1);
    assert.strictEqual(txn1.hasSync(key1), false);
    assert.strictEqual(txn1.getSync(key1), null);

    await txn1.close();
  });

  it('should insert and get the key', async () => {
    const txn1 = tree.txn();
    await txn1.open();

    const key1 = randomKey();
    const value = Buffer.from('Hello !');

    assert.strictEqual(await txn1.has(key1), false);
    await txn1.insert(key1, value);

    assert.strictEqual(await txn1.has(key1), true);
    const resValue = await txn1.get(key1);
    assert.bufferEqual(resValue, value);
    await txn1.remove(key1);
    assert.strictEqual(await txn1.has(key1), false);

    assert.strictEqual(await txn1.get(key1), null);

    await txn1.close();
  });

  it('should generate proofs', async () => {
    const txn1 = tree.txn();
    await txn1.open();

    const pairs = new Map();

    for (let i = 0; i < 10; i++) {
      const key = randomKey();
      const value = Buffer.from(`Hello ${i}.`);

      pairs.set(key.toString('hex'), value);
      await txn1.insert(key, value);
    }

    for (const keyHex of pairs.keys()) {
      const key = Buffer.from(keyHex, 'hex');
      const proof = await txn1.prove(key);
      const proofS = txn1.proveSync(key);

      assert.bufferEqual(proof, proofS);
    }

    await txn1.close();
  });

  it('should clear the transaction', async () => {
    const txn1 = tree.txn();
    await txn1.open();

    assert.bufferEqual(txn1.rootHash(), NULL_HASH);

    for (let i = 0; i < 10; i++) {
      const key = randomKey();
      const value = Buffer.from(`Hello ${i}.`);

      await txn1.insert(key, value);
    }

    assert.notBufferEqual(txn1.rootHash(), NULL_HASH);
    await txn1.clear();
    assert.bufferEqual(txn1.rootHash(), NULL_HASH);

    for (let i = 0; i < 10; i++) {
      const key = randomKey();
      const value = Buffer.from(`Hello ${i}.`);

      await txn1.insert(key, value);
    }

    txn1.clearSync();
    assert.bufferEqual(txn1.rootHash(), NULL_HASH);
  });
});
