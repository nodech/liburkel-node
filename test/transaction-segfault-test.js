'use strict';

const fs = require('fs');
const {testdir, rmTreeDir} = require('./util/common');
const {Tree} = require('../lib/tree');

// Tests for the segfaults that were encountered.
describe('Urkel Transaction (segfault)', function () {
  let prefix, tree;

  beforeEach(async () => {
    prefix = testdir('tx');
    fs.mkdirSync(prefix);

    tree = new Tree({prefix});
    await tree.open();
  });

  afterEach(async () => {
    if (tree.isOpen)
      await tree.close();

    if (fs.existsSync(prefix))
      rmTreeDir(prefix);
  });

  it('Segfault tx close from tree.close', async () => {
    let txn = tree.batch();
    await txn.open();
    await txn.close();

    txn = tree.txn();
    await txn.open();
    const snap = tree.snapshot();
    await snap.open();
    await snap.close();
    await tree.close();
  });
});
