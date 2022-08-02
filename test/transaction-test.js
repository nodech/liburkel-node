'use strict';

const path = require('path');
const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir, randomKey, sleep} = require('./util/common');
const {Tree} = require('../lib/tree');

const NULL_HASH = Buffer.alloc(32, 0);

describe('Urkel Transaction', function () {
  if (!global.gc)
    this.skip();

  let prefix, treeDir, tree;

  beforeEach(async () => {
    prefix = testdir('open');
    treeDir = path.join(prefix, 'tree');
    fs.mkdirSync(prefix);

    tree = new Tree({prefix});
    await tree.open();
  });

  afterEach(async () => {
    await tree.close();

    if (fs.existsSync(treeDir))
      rmTreeDir(treeDir);

    fs.rmdirSync(prefix);
  });

  it('should get the root', async () => {
    const txn1 = tree.txn();
    await txn1.maybeOpen();

    assert.bufferEqual(txn1.txRootHashSync(), NULL_HASH);
    assert.bufferEqual(await txn1.txRootHash(), NULL_HASH);
  });

  it('should insert and get the key (sync)', async () => {
    const txn1 = tree.txn();
    await txn1.maybeOpen();

    const key1 = randomKey();
    const value = Buffer.from('Hello !');

    assert.strictEqual(txn1.hasSync(key1), false);
    txn1.insertSync(key1, value);
    assert.strictEqual(txn1.hasSync(key1), true);
    const resValue = txn1.getSync(key1);
    assert.bufferEqual(resValue, value);
    txn1.removeSync(key1);
    assert.strictEqual(txn1.hasSync(key1), false);

    assert.throws(() => {
      txn1.getSync(key1);
    }, {
      message: 'URKEL_ENOTFOUND'
    });
  });
});
