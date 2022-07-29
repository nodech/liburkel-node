'use strict';

const path = require('path');
const fs = require('fs');
const {testdir, rmTreeDir, sleep} = require('./util/common');
const {Tree} = require('../lib/tree');

const SLEEP_BEFORE_GC = 200;
const SLEEP_AFTER_GC = 200;

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
    // TODO: Trigger close of related objects from the tree.
    this.skip();
    await tree.open();
    const transaction = tree.transaction();

    await transaction.maybeOpen();

    await tree.close();

    try {
      await transaction.close();
    } catch (e) {
      console.error(e);
    }
    // await assert.rejects(async () => {
    //   return await transaction.close();
    // }, {
    //   expect:
    // });
  });
});
