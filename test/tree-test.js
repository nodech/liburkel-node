'use strict';

const path = require('path');
const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir, randomKey} = require('./util/common');
const {Tree} = require('../lib/tree');

const NULL_HASH = Buffer.alloc(32, 0);

describe('Urkel Tree', function () {
  let prefix, treeDir, tree;

  beforeEach(async () => {
    prefix = testdir('open');
    treeDir = path.join(prefix, 'tree');
    fs.mkdirSync(prefix);

    tree = new Tree({ prefix });
    await tree.open();
  });

  afterEach(async () => {
    await tree.close();
    if (fs.existsSync(treeDir))
      rmTreeDir(treeDir);

    fs.rmdirSync(prefix);
  });

  it('should get tree root', async () => {
    assert.bufferEqual(tree.rootHash(), NULL_HASH);
    assert.bufferEqual(tree.treeRootHashSync(), NULL_HASH);
    assert.bufferEqual(await tree.treeRootHash(), NULL_HASH);
  });

  it('should fail to open in non-existent dir', async () => {
    const nprefix = path.join(prefix, 'non', 'existent', 'dir');
    const tree = new Tree({
      prefix: nprefix
    });

    let err;

    try {
      await tree.open();
    } catch (e) {
      err = e;
    }

    assert(err, 'tree open must fail');
    assert.strict(err.message, 'Urkel open failed.');
    assert.strict(err.code, 'URKEL_EBADOPEN');
  });

  it('should remove all tree files (sync)', async () => {
    const prefix = testdir('files');
    const treeDir = path.join(prefix, 'tree');
    fs.mkdirSync(prefix);

    const tree = new Tree({ prefix });
    await tree.open();
    await tree.close();

    assert.throws(() => {
      // NOTE: prefix is not the actual directory. it's tree
      // so it throws.
      Tree.destroySync(prefix);
    }, {
      code: 'URKEL_EBADOPEN',
      message: 'Urkel destroy failed.'
    });

    assert.deepStrictEqual(new Set(fs.readdirSync(treeDir)), new Set([
      'meta',
      '0000000001'
    ]));

    // This should destroy it.
    Tree.destroySync(treeDir);
    assert.strictEqual(fs.existsSync(treeDir), false);
    fs.rmdirSync(prefix);
  });

  it('should remove all tree files', async () => {
    const prefix = testdir('files');
    const treeDir = path.join(prefix, 'tree');
    fs.mkdirSync(prefix);

    const tree = new Tree({ prefix });
    await tree.open();
    await tree.close();

    assert.rejects(async () => {
      // NOTE: prefix is not the actual directory. it's tree
      // so it throws.
      await Tree.destroy(prefix);
    }, {
      code: 'URKEL_EBADOPEN',
      message: 'Urkel destroy failed.'
    });

    assert.deepStrictEqual(new Set(fs.readdirSync(treeDir)), new Set([
      'meta',
      '0000000001'
    ]));

    // This should destroy it.
    await Tree.destroy(treeDir);
    assert.strictEqual(fs.existsSync(treeDir), false);
    fs.rmdirSync(prefix);
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
      const valueS = tree.getSync(keys[i]);
      const value = await tree.get(keys[i]);

      assert.bufferEqual(valueS, origValue);
      assert.bufferEqual(value, origValue);
      assert.strictEqual(tree.hasSync(keys[i]), true);
      assert.strictEqual(await tree.has(keys[i]), true);
    }

    assert.throws(() => {
      tree.getSync(randomKey());
    }, {
      code: 'URKEL_ENOTFOUND',
      message: 'Failed to get.'
    });

    await assert.rejects(tree.get(randomKey()), {
      code: 'URKEL_ENOTFOUND',
      message: 'Failed to get.'
    });
  });

  it.only('should get proof', async () => {
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
        const key = randomKey();
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
        treeProofsSync[i * 5 + j] = tree.proveSync(key);;
        treeProofs[i * 5 + j] = await tree.prove(key);
        roots.push(tree.rootHash());
      }
    }

    let err = null;
    try {
      Tree.verifySync(proofs[0], keys[1], roots[0]);
    } catch (e) {
      err = e;
    }

    assert(err, 'Tree.verifySync should throw.');
    assert.strictEqual(err.message, 'Failed to verify_sync.');
    assert.strictEqual(err.code, 'URKEL_EHASHMISMATCH');

    err = null;
    try {
      await Tree.verify(proofs[0], keys[1], roots[0]);
    } catch (e) {
      err = e;
    }

    assert(err, 'Tree.verify should throw.');
    assert.strictEqual(err.message, 'Failed to verify.');
    assert.strictEqual(err.code, 'URKEL_EHASHMISMATCH');

    await assert.rejects(Tree.verify(proofs[0], keys[1], roots[0]), {
      code: 'URKEL_EHASHMISMATCH',
      message: 'Failed to verify.'
    });

    for (let i = 0; i < keys.length; i++) {
      const origValue = values[i];
      const root = roots[i];
      const proof = proofs[i];
      const treeProofSync = treeProofsSync[i];
      const treeProof = treeProofs[i];

      assert.bufferEqual(treeProofSync, proof);
      assert.bufferEqual(treeProof, proof);

      const [exitsSync, valueSync] = Tree.verifySync(proof, keys[i], root);
      const [exits, value] = await Tree.verify(proof, keys[i], root);

      assert.strictEqual(exitsSync, true);
      assert.strictEqual(exits, true);
      assert.bufferEqual(valueSync, origValue);
      assert.bufferEqual(value, origValue);
    }
  });
});
