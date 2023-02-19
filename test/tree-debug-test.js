'use strict';

const assert = require('assert');
const fs = require('fs');
const {testdir, rmTreeDir, isTreeDir, sleep} = require('./util/common');
const {Tree} = require('..');

describe('Nurkel debug state', function() {
  let prefix;

  beforeEach(() => {
    prefix = testdir('tree-gc');
    fs.mkdirSync(prefix);
  });

  afterEach(() => {
    if (isTreeDir(prefix))
      rmTreeDir(prefix);
  });

  it('should create tree', () => {
    const tree = new Tree({ prefix });

    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: 0,
      txs: 0,
      state: 'closed',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });
  });

  it('should open tree', async () => {
    const tree = new Tree({ prefix });

    const open = tree.open();
    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: 1,
      txs: 0,
      state: 'opening',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });

    await open;

    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: 0,
      txs: 0,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });
  });

  it('should close tree', async () => {
    const tree = new Tree({ prefix });
    await tree.open();

    const close = tree.close();
    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: 1,
      txs: 0,
      state: 'closing',
      isCloseQueued: true,
      // This is false, because there are no txs to close
      // and it unsets in the main thread from a single close call
      // w/o going into async.
      // If there were, this would be true.
      isTXCloseQueued: false,
      transactions: []
    });

    await close;

    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: 0,
      txs: 0,
      state: 'closed',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });
  });

  it('should queue multiple workers', async () => {
    const tree = new Tree({ prefix });
    await tree.open();

    const promises = [];
    for (let i = 0; i < 10; i++)
      promises.push(tree.get(Buffer.alloc(32, 0x00)));

    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: promises.length,
      txs: 0,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });

    await Promise.all(promises);

    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: 0,
      txs: 0,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });
  });

  it('should wait for workers before close', async () => {
    const tree = new Tree({ prefix });
    await tree.open();

    const promises = [];
    for (let i = 0; i < 10; i++)
      promises.push(tree.get(Buffer.alloc(32, 0x00)));

    const close = tree.close();

    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: promises.length,
      txs: 0,
      state: 'open',
      isCloseQueued: true,
      isTXCloseQueued: true,
      transactions: []
    });

    await Promise.all(promises);

    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: 1,
      txs: 0,
      state: 'closing',
      isCloseQueued: true,
      isTXCloseQueued: false,
      transactions: []
    });

    await close;

    assert.deepStrictEqual(tree.debugInfoSync(), {
      workers: 0,
      txs: 0,
      state: 'closed',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });
  });

  it('should wait for the transactions to close', async () => {
    const tree = new Tree({ prefix });
    await tree.open();

    const txn1 = tree.txn();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 0,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });

    await txn1.open();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 1,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: [{
        workers: 0,
        state: 'open',
        isCloseQueued: false
      }]
    });

    await txn1.close();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 0,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });

    await txn1.open();

    const close = tree.close();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 1,
      state: 'open',
      isCloseQueued: true,
      isTXCloseQueued: false,
      transactions: [{
        workers: 1,
        state: 'closing',
        isCloseQueued: true
      }]
    });

    await close;

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 0,
      state: 'closed',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });
  });

  it('should wait for the transactions to close (workers first)', async () => {
    const tree = new Tree({ prefix });
    await tree.open();

    const txn1 = tree.txn();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 0,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });

    await txn1.open();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 1,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: [{
        workers: 0,
        state: 'open',
        isCloseQueued: false
      }]
    });

    await txn1.close();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 0,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });

    await txn1.open();

    const promises = [];
    for (let i = 0; i < 10; i++)
      promises.push(tree.get(Buffer.alloc(32, 0x00)));

    const close = tree.close();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 10,
      txs: 1,
      state: 'open',
      isCloseQueued: true,
      isTXCloseQueued: true,
      transactions: [{
        workers: 0,
        state: 'open',
        isCloseQueued: false
      }]
    });

    await Promise.all(promises);

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 1,
      state: 'open',
      isCloseQueued: true,
      isTXCloseQueued: false,
      transactions: [{
        workers: 1,
        state: 'closing',
        isCloseQueued: true
      }]
    });

    await close;

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 0,
      state: 'closed',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });
  });

  it('should close tx when it is out of scope', async () => {
    if (!global.gc)
      this.skip();

    const tree = new Tree({ prefix });
    await tree.open();

    await (async () => {
      const txn1 = tree.txn();
      await txn1.open();

      const txn2 = tree.txn();
      await txn2.open();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        workers: 0,
        txs: 2,
        state: 'open',
        isCloseQueued: false,
        isTXCloseQueued: false,
        transactions: [{
          workers: 0,
          state: 'open',
          isCloseQueued: false
        },{
          workers: 0,
          state: 'open',
          isCloseQueued: false
        }]
      });
    })();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 2,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: [{
        workers: 0,
        state: 'open',
        isCloseQueued: false
      },{
        workers: 0,
        state: 'open',
        isCloseQueued: false
      }]
    });

    global.gc();

    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 2,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: [{
        workers: 0,
        state: 'open',
        isCloseQueued: false
      },{
        workers: 0,
        state: 'open',
        isCloseQueued: false
      }]
    });

    setImmediate(() => {
      assert.deepStrictEqual(tree.debugInfoSync(true), {
        workers: 0,
        txs: 2,
        state: 'open',
        isCloseQueued: false,
        isTXCloseQueued: false,
        transactions: [{
          workers: 1,
          state: 'closing',
          isCloseQueued: true
        },{
          workers: 1,
          state: 'closing',
          isCloseQueued: true
        }]
      });
    });

    await sleep(500);
    assert.deepStrictEqual(tree.debugInfoSync(true), {
      workers: 0,
      txs: 0,
      state: 'open',
      isCloseQueued: false,
      isTXCloseQueued: false,
      transactions: []
    });
  });
});
