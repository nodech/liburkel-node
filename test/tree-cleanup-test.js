'use strict';

const {testdir} = require('./util/common');
const nurkel = require('..');
const {runForked} = require('./util/worker');
const fs = require('bfile');

const cleanupTests = {
  openTree: {
    name: 'should clean up opened tree',
    fn: async (prefix) => {
      const tree = nurkel.create({ prefix });

      await tree.open();
    }
  },
  openMultipleTrees: {
    name: 'should clean up multiple opened trees',
    fn: async (prefix) => {
      for (let i = 0; i < 10; i++) {
        const prefixN = `${prefix}/${i}`;
        const tree = nurkel.create({ prefix: prefixN });

        await tree.open();
      }
    }
  },
  openClosingTree: {
    name: 'should clean up opened and closed tree',
    fn: async (prefix) => {
      const tree = nurkel.create({ prefix });

      await tree.open();
      tree.close();
    }
  },
  openClosingTrees: {
    name: 'should clean up multiple opened and closed trees',
    fn: async (prefix) => {
      for (let i = 0; i < 10; i++) {
        const prefixN = `${prefix}/${i}`;
        const tree = nurkel.create({ prefix: prefixN });

        await tree.open();
        tree.close();
      }
    }
  },
  openTreeTx: {
    name: 'should clean up opened tree with transaction',
    fn: async (prefix) => {
      const tree = nurkel.create({ prefix });

      await tree.open();

      const tx = tree.txn();
      await tx.open();
    }
  },
  openTreesTxs: {
    name: 'should clean up multiple opened trees with transactions',
    fn: async (prefix) => {
      for (let i = 0; i < 10; i++) {
        const prefixN = `${prefix}/${i}`;
        const tree = nurkel.create({ prefix: prefixN });

        await tree.open();

        const txs = [];
        for (let t = 0; t < 20; t++) {
          const tx = tree.txn();
          await tx.open();
          txs.push(tx);

          // close half
          if (t < 10)
            tx.close();
        }

        // close 10th tree
        if (i === 10)
          tree.close();
      }
    }
  },
  openTreeTxIter: {
    name: 'should clean up opened tree with transaction and iterator',
    fn: async (prefix) => {
      const tree = nurkel.create({ prefix });

      await tree.open();

      const tx = tree.txn();
      await tx.open();

      tx.iterator();

      // TODO: separate iterator init from open.
      // const iter = tx.iterator();
      // await iter.open();
    }
  },
  openTreesTxsIters: {
    name: 'should clean up multiple opened trees with transactions and iterators',
    fn: async (prefix) => {
      for (let i = 0; i < 10; i++) {
        const tree = nurkel.create({ prefix: `${prefix}/${i}` });

        await tree.open();

        for (let t = 0; t < 20; t++) {
          const tx = tree.txn();
          await tx.open();

          for (let j = 0; j < 20; j++) {
            const iter = tx.iterator();

            if (j === 10)
              iter.end();
          }

          if (t < 10)
            tx.close();
        }
      }
    },
    timeout: 3000
  }
};

const isForked = Boolean(process.send);
const isMainThread = !isForked;

if (!isMainThread) {
  const data = JSON.parse(process.env.WORKER_DATA);

  // select the cleanup test
  const {
    testName,
    prefix
  } = data;

  const testCase = cleanupTests[testName];

  if (!testCase)
    throw new Error(`Test ${testName} not found`);

  testCase.fn(prefix).catch((e) => {
    console.error(e);
    process.exit(1);
  });

  return;
}

// Main Thread runs the tests.
describe.skip('Tree env cleanup', function() {
  let prefix;

  beforeEach(() => {
    prefix = testdir('cleanup-tree');
  });

  afterEach(async () => {
    await fs.rimraf(prefix);
  });

  for (const [id, testDetails] of Object.entries(cleanupTests)) {
    it(testDetails.name, async () => {
      if (testDetails.timeout != null)
        this.timeout(testDetails.timeout);

      await runForked(__filename, {
        testName: id,
        prefix: prefix
      }, testDetails.timeout);
    });
  }
});
