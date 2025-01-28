'use strict';

const path = require('path');
const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir, isTreeDir, randomKey} = require('./util/common');
const nurkel = require('..');
const {statusCodes} = nurkel;

const NULL_HASH = Buffer.alloc(32, 0);

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
describe(`Urkel Tree (${name})`, function () {
    this.timeout(0);
  const Tree = name === 'nurkel' ? nurkel.Tree : nurkel.UrkelTree;
  let prefix, tree;

  beforeEach(async () => {
    prefix = testdir('tree');
    tree = nurkel.create({ prefix, ...treeTestOptions });
    await tree.open();
  });

  afterEach(async () => {
    await tree.close();

    if (isTreeDir(prefix))
      rmTreeDir(prefix);
  });

  it('should get tree root', async () => {
    assert.bufferEqual(tree.rootHash(), NULL_HASH);
    assert.bufferEqual(tree.treeRootHashSync(), NULL_HASH);
    assert.bufferEqual(await tree.treeRootHash(), NULL_HASH);
  });

  it('should fail to open twice', async () => {
    let err;

    try {
      await tree.open();
    } catch (e) {
      err = e;
    }

    assert(err, 'tree open must fail.');
  });

  it('should fail to open locked tree', async () => {
    if (treeTestOptions.memory)
      this.skip();

    const tree2 = nurkel.create({ prefix, ...treeTestOptions });

    let err;

    try {
      await tree2.open();
    } catch (e) {
      err = e;
    }

    // it is locked by another tree instance.
    assert(err, 'tree open must fail.');
  });

  it('should open a non-existent dir', async () => {
    if (treeTestOptions.memory)
      this.skip();

    const nprefix = path.join(prefix, 'non-existent', 'dir');
    const tree = nurkel.create({ prefix: nprefix, ...treeTestOptions });

    await tree.open();

    await tree.close();

    rmTreeDir(nprefix);
    fs.rmdirSync(path.dirname(nprefix));
  });

  it('should remove all tree files (sync)', async () => {
    if (treeTestOptions.memory || treeTestOptions.urkel)
      this.skip();

    const prefix = testdir('files');
    fs.mkdirSync(prefix);

    const tree = nurkel.create({ prefix, ...treeTestOptions });
    await tree.open();
    await tree.close();

    assert.throws(() => {
      // NOTE: prefix/tree is not the actual directory. it's prefix itself.
      Tree.destroySync(path.join(prefix, 'tree'));
    }, {
      code: 'URKEL_EBADOPEN',
      message: 'Urkel destroy failed.'
    });

    assert.deepStrictEqual(new Set(fs.readdirSync(prefix)), new Set([
      'meta',
      '0000000001'
    ]));

    // This should destroy it.
    Tree.destroySync(prefix);
    assert.strictEqual(fs.existsSync(prefix), false);
  });

  it('should remove all tree files', async () => {
    if (treeTestOptions.memory)
      this.skip();

    const prefix = testdir('files');
    fs.mkdirSync(prefix);

    const tree = nurkel.create({ prefix, ...treeTestOptions });
    await tree.open();
    await tree.close();

    let err;
    try {
      // NOTE: prefix is not the actual directory. it's tree
      // so it throws.
      await Tree.destroy(path.join(prefix, 'tree'));
    } catch (e) {
      err = e;
    }

    assert(err);

    assert.deepStrictEqual(new Set(fs.readdirSync(prefix)), new Set([
      'meta',
      '0000000001'
    ]));

    // This should destroy it.
    await Tree.destroy(prefix);
    assert.strictEqual(fs.existsSync(prefix), false);
  });

  it('should get values', async () => {
    const txn = tree.txn();
    const keys = [];
    const values = [];
    await txn.open();

    for (let i = 0; i < 10; i++) {
      const key = randomKey();
      const value = Buffer.from(`value ${i}.`);

      keys[i] = key;
      values[i] = value;

      await txn.insert(key, value);
    }

    await txn.commit();
    await txn.close();

    for (let i = 0; i < 10; i++) {
      const origValue = values[i];
      const value = await tree.get(keys[i]);

      if (tree.supportsSync) {
        const valueS = tree.getSync(keys[i]);
        assert.bufferEqual(valueS, origValue);
      }

      assert.bufferEqual(value, origValue);
      if (tree.supportsSync)
        assert.strictEqual(tree.hasSync(keys[i]), true);
      assert.strictEqual(await tree.has(keys[i]), true);
    }

    if (tree.supportsSync)
      assert.strictEqual(tree.getSync(randomKey()), null);
    assert.strictEqual(await tree.get(randomKey()), null);
  });

  it('should get proof', async () => {
    const keys = [];
    const values = [];
    const proofs = [];
    const treeProofs = [];
    const treeProofsSync = [];
    const roots = [];

    for (let i = 0; i < 5; i++) {
      const txn = tree.txn();
      await txn.open();

      for (let j = 0; j < 5; j++) {
        const key = Buffer.alloc(32, i * 5 + j);
        const value = Buffer.from(`Value: ${i}.${j}.`);

        await txn.insert(key, value);
        keys[i * 5 + j] = key;
        values[i * 5 + j] = value;
      }

      for (let j = 0; j < 5; j++) {
        const key = keys[i * 5 + j];
        const proof = await txn.prove(key);
        proofs[i * 5 + j] = proof;
      }

      await txn.commit();
      for (let j = 0; j < 5; j++) {
        const key = keys[i * 5 + j];
        if (tree.supportsSync)
          treeProofsSync[i * 5 + j] = tree.proveSync(key);;
        treeProofs[i * 5 + j] = await tree.prove(key);
        roots.push(tree.rootHash());
      }
    }

    let code, value;
    if (tree.supportsSync) {
      [code, value] = Tree.verifySync(roots[0], keys[1], proofs[0]);
      assert.strictEqual(value, null);
      assert.strictEqual(code, statusCodes.URKEL_EHASHMISMATCH);
    }

    [code, value] = await Tree.verify(roots[0], keys[1], proofs[0]);
    assert.strictEqual(value, null);
    assert.strictEqual(code, statusCodes.URKEL_EHASHMISMATCH);

    for (let i = 0; i < keys.length; i++) {
      const origValue = values[i];
      const root = roots[i];
      const proof = proofs[i];
      const treeProofSync = treeProofsSync[i];
      const treeProof = treeProofs[i];

      if (tree.supportsSync)
        assert.bufferEqual(treeProofSync.raw, proof.raw);
      assert.bufferEqual(treeProof.raw, proof.raw);

      if (tree.supportsSync) {
        const [codeSync, valueSync] = Tree.verifySync(
          root,
          keys[i],
          proof
        );

        assert.strictEqual(codeSync, statusCodes.URKEL_OK);
        assert.bufferEqual(valueSync, origValue);
      }

      {
        const [code, value] = await Tree.verify(root, keys[i], proof);
        assert.strictEqual(code, statusCodes.URKEL_OK);
        assert.bufferEqual(value, origValue);
      }
      break;
    }
  });

  it('should inject', async () => {
    // 5 roots with 5 entries
    const ROOTS = 5;
    const ENTRIES = 5;
    const roots = [];
    const entriesByRoot = [];

    const txn = tree.batch();
    await txn.open();

    for (let i = 0; i < ROOTS; i++) {
      const entries = [];
      for (let j = 0; j < ENTRIES; j++) {
        const key = randomKey();
        const value = Buffer.from(`value ${i * 10 + j}.`);

        entries.push([key, value]);
        await txn.insert(key, value);
      }

      roots.push(await txn.commit());
      entriesByRoot.push(entries);
    }

    await txn.close();
    const last = tree.rootHash();

    for (let i = ROOTS - 1; i >= 0; i--) {
      // go to the past.
      await tree.inject(roots[i]);

      for (const [rootIndex, entries] of entriesByRoot.entries()) {
        if (rootIndex > i) {
          for (const [key] of entries) {
            assert.strictEqual(await tree.has(key), false);
            assert.strictEqual(await tree.get(key), null);
            if (tree.supportsSync) {
              assert.strictEqual(tree.hasSync(key), false);
              assert.strictEqual(tree.getSync(key), null);
            }
          }
        } else {
          for (const [key, value] of entries) {
            assert.strictEqual(await tree.has(key), true);
            assert.bufferEqual(await tree.get(key), value);
            if (tree.supportsSync) {
              assert.strictEqual(tree.hasSync(key), true);
              assert.bufferEqual(tree.getSync(key), value);
            }
          }
        }
      }
    }

    await tree.inject(last);

    for (const [key, value] of entriesByRoot.flat()) {
      assert.strictEqual(await tree.has(key), true);
      assert.bufferEqual(await tree.get(key), value);
      if (tree.supportsSync) {
        assert.strictEqual(tree.hasSync(key), true);
        assert.bufferEqual(tree.getSync(key), value);
      }
    }

    for (let i = ROOTS - 1; i >= 0; i--) {
      // go to the past.
      await tree.inject(roots[i]);

      for (const [rootIndex, entries] of entriesByRoot.entries()) {
        if (rootIndex > i) {
          for (const [key] of entries) {
            assert.strictEqual(await tree.has(key), false);
            assert.strictEqual(await tree.get(key), null);
            if (tree.supportsSync) {
              assert.strictEqual(tree.hasSync(key), false);
              assert.strictEqual(tree.getSync(key), null);
            }
          }
        } else {
          for (const [key, value] of entries) {
            assert.strictEqual(await tree.has(key), true);
            assert.bufferEqual(await tree.get(key), value);
            if (tree.supportsSync) {
              assert.strictEqual(tree.hasSync(key), true);
              assert.bufferEqual(tree.getSync(key), value);
            }
          }
        }
      }
    }

    if (tree.supportsSync)
      tree.injectSync(last);
    else
      await tree.inject(last);

    for (const [key, value] of entriesByRoot.flat()) {
      assert.strictEqual(await tree.has(key), true);
      assert.bufferEqual(await tree.get(key), value);
      if (tree.supportsSync) {
        assert.strictEqual(tree.hasSync(key), true);
        assert.bufferEqual(tree.getSync(key), value);
      }
    }
  });
});
}
