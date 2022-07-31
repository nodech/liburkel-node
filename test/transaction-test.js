'use strict';

const path = require('path');
const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir} = require('./util/common');
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
  });

  afterEach(async () => {
    if (fs.existsSync(treeDir))
      rmTreeDir(treeDir);

    fs.rmdirSync(prefix);
    global.gc();
  });

  it('should init transaction and cleanup', async () => {
    await tree.open();
    {
      tree.transaction();
    }
    global.gc();
    await tree.close();
  });

  it('should open and close transaction', async () => {
    await tree.open();

    const transaction = tree.transaction();
    await transaction.maybeOpen();
    await transaction.close();

    await tree.close();
  });

  it('should init transaction, open and cleanup', async () => {
    await tree.open();

    // Make sure it goes out of scope
    await (async () => {
      const transaction = tree.transaction();
      await transaction.maybeOpen();
    })();

    global.gc();
    await (async () => {
      const transaction = tree.transaction();
      // Don't wait
      transaction.maybeOpen();
    })();

    global.gc();

    await tree.close();
  });

  it('should close transaction when tree closes', async () => {
    await tree.open();
    const transaction = tree.transaction();

    await transaction.maybeOpen();
    await tree.close();

    await assert.rejects(async () => {
      return await transaction.close();
    }, {
      message: 'Transaction is not ready.'
    });
  });

  it('should close all transactions', async () => {
    await tree.open();

    const txns = [];

    for (let i = 0; i < 10; i++) {
      txns.push(tree.transaction());

      // Open and wait 5.
      if (i < 5) {
        await txns[i].maybeOpen();
      }

      // wait close 2.
      if (i < 2) {
        await txns[i].close();
      }

      // don't wait close 2.
      if (i >= 2 && i <= 3) {
        txns[i].close();
      }

      // don't wait open >5
      if (i >= 5)
        txns[i].maybeOpen();
    }

    await tree.close();

    for (const tx of txns) {
      await assert.rejects(async () => {
        return await tx.close();
      }, {
        message: 'Transaction is not ready.'
      });
    }
  });

  it('should close tx and tree after out-of-scope', async () => {
    await tree.open();

    await (async () => {
      for (let i = 0; i < 100; i++) {
        const txn = await tree.txn();
        await txn.maybeOpen();
      }

      for (let i = 0; i < 100; i++) {
        const txn = await tree.txn();
        await txn.maybeOpen();
        txn.close();
      }
    })();

    global.gc();
    await tree.close();
  });

  it('should get the root', async () => {
    await tree.open();

    const txn1 = tree.txn();
    await txn1.maybeOpen();

    assert.bufferEqual(txn1.txRootHashSync(), NULL_HASH);
    assert.bufferEqual(await txn1.txRootHash(), NULL_HASH);
    await tree.close();
  });
});
