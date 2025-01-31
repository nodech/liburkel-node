'use strict';

const assert = require('bsert');
const {testdir, rmTreeDir, isTreeDir} = require('./util/common');
const nurkel = require('..');

const keys = {
  6: Buffer.alloc(32, 0x06),
  5: Buffer.alloc(32, 0x05),
  4: Buffer.alloc(32, 0x04),
  3: Buffer.alloc(32, 0x03),
  2: Buffer.alloc(32, 0x02),
  1: Buffer.alloc(32, 0x01),
  0: Buffer.alloc(32, 0x00)
};

const treeCreateOptions = {
  'nurkel': {},
  // use legacy tree
  'urkel': {
    urkel: true
  },
  // legacy tree as in memory tree
  'memory': {
    memory: true
  }
};

for (const [name, treeTestOptions] of Object.entries(treeCreateOptions)) {
describe(`Urkel Iterator (${name})`, function () {
  let prefix, tree;

  const addEntries = async () => {
    const txn = tree.txn();
    await txn.open();

    for (const [i, key] of Object.entries(keys)) {
      const value = Buffer.from(`Value: ${i}`);
      await txn.insert(key, value);
    }

    await txn.commit();
    await txn.close();
  };

  beforeEach(async () => {
    prefix = testdir('tx');

    tree = nurkel.create({ prefix, ...treeTestOptions });
    await tree.open();

    await addEntries();
  });

  afterEach(async () => {
    await tree.close();

    if (isTreeDir(prefix))
      rmTreeDir(prefix);
  });

  it('should not create iterator from closed transaction', async () => {
    const txn = tree.txn();

    // This comes from the js guard.
    assert.throws(() => {
      txn.iterator();
    }, {
      message: 'Transaction is not open.'
    });
  });

  it('should check internal state', () => {
    // only applies to nurkel tree.
    if (!tree.supportsSync)
      this.skip();

    const txn = tree.txn();

    // Transaction is still closed, but check the nurkel side of things.
    txn.isOpen = true;

    let err;

    try {
      txn.iterator();
    } catch (e) {
      err = e;
    }

    assert(err, 'should throw error');
    assert.strictEqual(err.message, 'Transaction is closed.');
  });

  it('should create an iterator from snapshot and iterate sync', async () => {
    if (!tree.supportsSync)
      this.skip();
    const snap = tree.snapshot();
    await snap.open();

    const iter = snap.iterator();

    let i = 0;
    for (const [key, value] of iter) {
      assert.bufferEqual(key, keys[i]);
      assert.bufferEqual(value, Buffer.from(`Value: ${i++}`));
    }
    assert.strictEqual(i, Object.keys(keys).length);

    await iter.end();
    await snap.close();
  });

  it('should create an iterator from snapshot and iterate async', async () => {
    const snap = tree.snapshot();
    await snap.open();

    const iter = snap.iterator();

    let i = 0;
    for await (const [key, value] of iter) {
      assert.bufferEqual(key, keys[i]);
      assert.bufferEqual(value, Buffer.from(`Value: ${i++}`));
    }
    assert.strictEqual(i, Object.keys(keys).length);

    await iter.end();
    await snap.close();
  });
});
}
