'use strict';

const path = require('path');
const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir} = require('./util/common');
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
});
