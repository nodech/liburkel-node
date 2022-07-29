'use strict';

const path = require('path');
const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir, sleep} = require('./util/common');
const {Tree} = require('../lib/tree');

const SLEEP_BEFORE_GC = 0;
const SLEEP_AFTER_GC = 0;

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
    await sleep(SLEEP_AFTER_GC);
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

    await sleep(SLEEP_BEFORE_GC);
    global.gc();
    await sleep(SLEEP_AFTER_GC);
    await (async () => {
      const transaction = tree.transaction();
      // Don't wait
      transaction.maybeOpen();
    })();

    await sleep(SLEEP_BEFORE_GC);
    global.gc();
    await sleep(SLEEP_AFTER_GC);

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
    this.timeout(5000);
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
});
