'use strict';

const assert = require('assert');
const fs = require('fs');
const {testdir, rmTreeDir, isTreeDir, sleep} = require('./util/common');
const {Tree} = require('..');

const CLOSED_TREE = {
  workers: 0,
  txs: 0,
  state: 'closed',
  isCloseQueued: false,
  isTXCloseQueued: false
};

const CLOSED_TX = {
  workers: 0,
  iters: 0,
  state: 'closed',
  isCloseQueued: false,
  isIterCloseQueued: false
};

const OPEN_ITER = {
  nexting: false,
  state: 'open',
  isCloseQueued: false,
  cacheMaxSize: 1,
  cacheSize: 0,
  bufferSize: 1064
};

describe('Nurkel debug state', function() {
  let prefix, tree;

  const before = () => {
    prefix = testdir('tree-gc');
    fs.mkdirSync(prefix);

    tree = new Tree({ prefix });
  };

  const after = () => {
    if (isTreeDir(prefix))
      rmTreeDir(prefix);
  };

  describe('Tree', function() {
    beforeEach(before);
    afterEach(after);

    it('should create tree', () => {
      assert.deepStrictEqual(tree.debugInfoSync(), CLOSED_TREE);
    });

    it('should open tree', async () => {
      const open = tree.open();
      assert.deepStrictEqual(tree.debugInfoSync(), {
        ...CLOSED_TREE,
        workers: 1,
        state: 'opening'
      });

      await open;

      assert.deepStrictEqual(tree.debugInfoSync(), {
        ...CLOSED_TREE,
        state: 'open'
      });
    });

    it('should close tree', async () => {
      await tree.open();

      const close = tree.close();
      assert.deepStrictEqual(tree.debugInfoSync(), {
        ...CLOSED_TREE,
        workers: 1,
        state: 'closing',
        isCloseQueued: true,
        // This is false, because there are no txs to close
        // and it unsets in the main thread from a single close call
        // w/o going into async.
        // If there were, this would be true.
        isTXCloseQueued: false
      });

      await close;

      assert.deepStrictEqual(tree.debugInfoSync(), CLOSED_TREE);
    });

    it('should queue multiple workers', async () => {
      await tree.open();

      const promises = [];
      for (let i = 0; i < 10; i++)
        promises.push(tree.get(Buffer.alloc(32, 0x00)));

      assert.deepStrictEqual(tree.debugInfoSync(), {
        ...CLOSED_TREE,
        workers: promises.length,
        state: 'open'
      });

      await Promise.all(promises);

      assert.deepStrictEqual(tree.debugInfoSync(), {
        ...CLOSED_TREE,
        state: 'open'
      });
    });

    it('should wait for workers before close', async () => {
      await tree.open();

      const promises = [];
      for (let i = 0; i < 10; i++)
        promises.push(tree.get(Buffer.alloc(32, 0x00)));

      const close = tree.close();

      assert.deepStrictEqual(tree.debugInfoSync(), {
        ...CLOSED_TREE,
        workers: promises.length,
        state: 'open',
        isCloseQueued: true,
        isTXCloseQueued: true
      });

      await Promise.all(promises);

      assert.deepStrictEqual(tree.debugInfoSync(), {
        ...CLOSED_TREE,
        workers: 1,
        state: 'closing',
        isCloseQueued: true,
        isTXCloseQueued: false
      });

      await close;

      assert.deepStrictEqual(tree.debugInfoSync(), CLOSED_TREE);
    });
  });

  describe('Transaction', function() {
    beforeEach(before);
    afterEach(after);

    it('should wait for the transactions to close', async () => {
      await tree.open();

      const txn1 = tree.txn();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        state: 'open',
        transactions: []
      });

      await txn1.open();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        txs: 1,
        state: 'open',
        transactions: [{
          ...CLOSED_TX,
          state: 'open'
        }]
      });

      await txn1.close();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        state: 'open',
        transactions: []
      });

      await txn1.open();

      const close = tree.close();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        txs: 1,
        state: 'open',
        isCloseQueued: true,
        isTXCloseQueued: false,
        transactions: [{
          ...CLOSED_TX,
          workers: 1,
          state: 'closing',
          isCloseQueued: true
        }]
      });

      await close;

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        transactions: []
      });
    });

    it('should wait for the transactions to close (workers first)', async () => {
      await tree.open();

      const txn1 = tree.txn();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        state: 'open',
        transactions: []
      });

      await txn1.open();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        txs: 1,
        state: 'open',
        transactions: [{
          ...CLOSED_TX,
          state: 'open'
        }]
      });

      await txn1.close();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        state: 'open',
        transactions: []
      });

      await txn1.open();

      const promises = [];
      for (let i = 0; i < 10; i++)
        promises.push(tree.get(Buffer.alloc(32, 0x00)));

      const close = tree.close();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        workers: 10,
        txs: 1,
        state: 'open',
        isCloseQueued: true,
        isTXCloseQueued: true,
        transactions: [{
          ...CLOSED_TX,
          state: 'open'
        }]
      });

      await Promise.all(promises);

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        txs: 1,
        state: 'open',
        isCloseQueued: true,
        isTXCloseQueued: false,
        transactions: [{
          ...CLOSED_TX,
          workers: 1,
          state: 'closing',
          isCloseQueued: true
        }]
      });

      await close;

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        transactions: []
      });
    });

    it('should close tx when it is out of scope', async () => {
      if (!global.gc)
        this.skip();

      await tree.open();

      await (async () => {
        const txn1 = tree.txn();
        await txn1.open();

        const txn2 = tree.txn();
        await txn2.open();

        assert.deepStrictEqual(tree.debugInfoSync(true), {
          ...CLOSED_TREE,
          txs: 2,
          state: 'open',
          transactions: [{
            ...CLOSED_TX,
            state: 'open'
          },{
            ...CLOSED_TX,
            state: 'open'
          }]
        });
      })();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        txs: 2,
        state: 'open',
        transactions: [{
          ...CLOSED_TX,
          state: 'open'
        },{
          ...CLOSED_TX,
          state: 'open'
        }]
      });

      global.gc();

      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        txs: 2,
        state: 'open',
        transactions: [{
          ...CLOSED_TX,
          state: 'open'
        },{
          ...CLOSED_TX,
          state: 'open'
        }]
      });

      setImmediate(() => {
        assert.deepStrictEqual(tree.debugInfoSync(true), {
          ...CLOSED_TREE,
          txs: 2,
          state: 'open',
          transactions: [{
            ...CLOSED_TX,
            workers: 1,
            state: 'closing',
            isCloseQueued: true
          },{
            ...CLOSED_TX,
            workers: 1,
            state: 'closing',
            isCloseQueued: true
          }]
        });
      });

      await sleep(100);
      assert.deepStrictEqual(tree.debugInfoSync(true), {
        ...CLOSED_TREE,
        state: 'open',
        transactions: []
      });
    });
  });

  describe('Iterator', function() {
    beforeEach(before);
    afterEach(after);

    it('should create iterator', async () => {
      await tree.open();

      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        state: 'open',
        transactions: []
      });

      const txn = tree.txn();
      await txn.open();

      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        state: 'open',
        txs: 1,
        transactions: [{
          ...CLOSED_TX,
          state: 'open',
          iterators: []
        }]
      });

      // Now create iterator
      const iter = txn.iterator();
      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        state: 'open',
        txs: 1,
        transactions: [{
          ...CLOSED_TX,
          state: 'open',
          iters: 1,
          iterators: [{
            ...OPEN_ITER
          }]
        }]
      });

      // Should close iterator
      await iter.end();
      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        state: 'open',
        txs: 1,
        transactions: [{
          ...CLOSED_TX,
          state: 'open',
          iters: 0,
          iterators: []
        }]
      });
    });

    it('should close iterator on txn close', async () => {
      await tree.open();

      const txn = tree.transaction()
      await txn.open();

      // Let's create second one.
      const iter = txn.iterator();

      // Keep worke busy.
      const get = txn.get(Buffer.alloc(32, 0x00));

      // Let's close transaction now, but don't wait
      const close = txn.close();

      // TXN will wait for workers first.
      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        state: 'open',
        txs: 1,
        transactions: [{
          ...CLOSED_TX,
          workers: 1,
          state: 'open',
          isCloseQueued: true,
          isIterCloseQueued: true,
          iters: 1,
          iterators: [{
            ...OPEN_ITER,
            state: 'open'
          }]
        }]
      });

      await get;

      // Now there's no worker, we are closing iterators.
      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        state: 'open',
        txs: 1,
        transactions: [{
          ...CLOSED_TX,
          state: 'open',
          isCloseQueued: true,
          // Close is already running
          isIterCloseQueued: false,
          iters: 1,
          iterators: [{
            ...OPEN_ITER,
            state: 'closing',
            nexting: true,
            isCloseQueued: true
          }]
        }]
      });

      let err;
      try {
        await iter.end();
      } catch (e) {
        err = e;
      }

      assert(err);
      assert.strictEqual(err.message, 'is closing.');

      await close;

      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        state: 'open',
        txs: 0,
        transactions: []
      });
    });

    it('should close everything on tree close', async () => {
      await tree.open();

      const txn = tree.transaction();
      await txn.open();

      txn.iterator();

      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        state: 'open',
        txs: 1,
        transactions: [{
          ...CLOSED_TX,
          state: 'open',
          iters: 1,
          iterators: [{
            ...OPEN_ITER
          }]
        }]
      });

      tree.close();

      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        state: 'open',
        txs: 1,
        isCloseQueued: true,
        transactions: [{
          ...CLOSED_TX,
          state: 'open',
          iters: 1,
          isCloseQueued: true,
          // has already run
          isIterCloseQueued: false,
          iterators: [{
            ...OPEN_ITER,
            state: 'closing',
            nexting: true,
            isCloseQueued: true
          }]
        }]
      });

      await sleep(100);
      assert.deepStrictEqual(tree.debugInfoSync(true, true), {
        ...CLOSED_TREE,
        transactions: []
      });
    });

    it('should start clean up when iter is out of scope', async () => {
      if (!global.gc)
        this.skip();

      await tree.open();
      const txn = tree.transaction();
      await txn.open();

      await (async () => {
        txn.iterator();
      })();

      global.gc();

      setImmediate(() => {
        assert.deepStrictEqual(tree.debugInfoSync(true, true), {
          ...CLOSED_TREE,
          state: 'open',
          txs: 1,
          transactions: [{
            ...CLOSED_TX,
            state: 'open',
            iters: 1,
            iterators: [{
              ...OPEN_ITER,
              nexting: true,
              state: 'closing',
              isCloseQueued: true
            }]
          }]
        });
      });
    });
  });
});
