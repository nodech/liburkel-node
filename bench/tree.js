/* eslint no-unused-vars: "off" */
/* eslint no-implicit-coercion: "off" */

'use strict';

const assert = require('bsert');
const Path = require('path');
const crypto = require('crypto');
const util = require('./util');

const argv = process.argv.slice();

const {Tree} = require('../lib/tree');

const BLOCKS = +argv[3] || 10000;
const PER_BLOCK = +argv[4] || 300;
const INTERVAL = +argv[5] || 72;
const TOTAL = BLOCKS * PER_BLOCK;
const FILE = Path.resolve(__dirname, 'treedb');

async function verify(root, key, proof) {
  return Tree.verify(root, key, proof);
}

async function stress(prefix) {
  const tree = new Tree({ prefix });

  await tree.open();

  const batch = tree.batch();
  await batch.open();

  console.log(
    'Committing %d values to tree at a rate of %d per block.',
    TOTAL,
    PER_BLOCK);

  for (let i = 0; i < BLOCKS; i++) {
    const pairs = [];

    for (let j = 0; j < PER_BLOCK; j++) {
      const k = crypto.randomBytes(tree.bits >>> 3);
      const v = crypto.randomBytes(300);

      pairs.push([k, v]);
    }

    {
      const now = util.now();

      for (const [k, v] of pairs)
        await batch.insert(k, v);

      batch.rootHash();

      console.log('Insertion: %d', util.now() - now);
    }

    const [key, value] = pairs.pop();

    pairs.length = 0;

    if (i === 0)
      continue;

    if ((i % INTERVAL) === 0) {
      util.memory();

      const now = util.now();

      await batch.commit();

      console.log('Commit: %d', util.now() - now);

      util.logMemory();

      await doProof(tree, i, key, value);
    }

    if ((i % 100) === 0)
      console.log('Keys: %d', i * PER_BLOCK);
  }

  console.log('Total Items: %d.', TOTAL);
  console.log('Blocks: %d.', BLOCKS);
  console.log('Items Per Block: %d.', PER_BLOCK);

  await batch.close();
  return tree.close();
}

async function doProof(tree, i, key, expect) {
  const now = util.now();
  const proof = await tree.prove(key);

  console.log('Proof %d time: %d.', i, util.now() - now);

  const [code, value] = await verify(tree.rootHash(), key, proof);

  assert.strictEqual(code, 0);
  assert.notStrictEqual(value, null);
  assert(Buffer.isBuffer(value));
  assert.strictEqual(value.length, 300);
  assert.bufferEqual(value, expect);

  console.log('Proof %d size: %d', i, proof.length);
}

async function bench(prefix) {
  const tree = new Tree({ prefix });
  const items = [];

  await tree.open();

  let batch = tree.batch();
  await batch.open();

  for (let i = 0; i < 100000; i++) {
    const r = Math.random() > 0.5;
    const key = crypto.randomBytes(tree.bits >>> 3);
    const value = crypto.randomBytes(r ? 100 : 1);

    items.push([key, value]);
  }

  {
    const now = util.now();

    for (const [key, value] of items)
      await batch.insert(key, value);

    console.log('Insert: %d.', util.now() - now);
  }

  {
    const now = util.now();

    for (const [key] of items)
      assert(await batch.get(key));

    console.log('Get (cached): %d.', util.now() - now);
  }

  {
    const now = util.now();

    await batch.commit();

    console.log('Commit: %d.', util.now() - now);
  }

  await batch.close();
  await tree.close();
  await tree.open();

  {
    const now = util.now();

    for (const [key] of items)
      assert(await tree.get(key));

    console.log('Get (uncached): %d.', util.now() - now);
  }

  batch = tree.batch();
  await batch.open();

  {
    const now = util.now();

    for (let i = 0; i < items.length; i++) {
      const [key] = items[i];

      if (i & 1)
        await batch.remove(key);
    }

    console.log('Remove: %d.', util.now() - now);
  }

  {
    const now = util.now();

    await batch.commit();

    console.log('Commit: %d.', util.now() - now);
  }

  {
    const now = util.now();

    await batch.commit();

    console.log('Commit (nothing): %d.', util.now() - now);
  }

  await tree.close();
  await tree.open();

  {
    const root = tree.rootHash();
    const [key] = items[items.length - 100];

    let proof = null;

    {
      const now = util.now();

      proof = await tree.prove(key);

      console.log('Proof: %d.', util.now() - now);
    }

    {
      const now = util.now();
      const [code, value] = await verify(root, key, proof);
      assert(code === 0);
      assert(value);
      console.log('Verify: %d.', util.now() - now);
    }
  }

  return tree.close();
}

async function benchVTX(prefix) {
  const tree = new Tree({ prefix });
  const items = [];

  await tree.open();

  let batch = tree.vbatch();
  await batch.open();

  for (let i = 0; i < 100000; i++) {
    const r = Math.random() > 0.5;
    const key = crypto.randomBytes(tree.bits >>> 3);
    const value = crypto.randomBytes(r ? 100 : 1);

    items.push([key, value]);
  }

  {
    const now = util.now();

    for (const [key, value] of items)
      await batch.insert(key, value);

    console.log('Insert: %d.', util.now() - now);
  }

  {
    const now = util.now();

    for (const [key] of items)
      assert(await batch.get(key));

    console.log('Get (cached): %d.', util.now() - now);
  }

  {
    const now = util.now();

    await batch.commit();

    console.log('Commit: %d.', util.now() - now);
  }

  await batch.close();
  await tree.close();
  await tree.open();

  {
    const now = util.now();

    for (const [key] of items)
      assert(await tree.get(key));

    console.log('Get (uncached): %d.', util.now() - now);
  }

  batch = tree.vbatch();
  await batch.open();

  {
    const now = util.now();

    for (let i = 0; i < items.length; i++) {
      const [key] = items[i];

      if (i & 1)
        await batch.remove(key);
    }

    console.log('Remove: %d.', util.now() - now);
  }

  {
    const now = util.now();

    await batch.commit();

    console.log('Commit: %d.', util.now() - now);
  }

  {
    const now = util.now();

    await batch.commit();

    console.log('Commit (nothing): %d.', util.now() - now);
  }

  await tree.close();
  await tree.open();

  {
    const root = tree.rootHash();
    const [key] = items[items.length - 100];

    let proof = null;

    {
      const now = util.now();

      proof = await tree.prove(key);

      console.log('Proof: %d.', util.now() - now);
    }

    {
      const now = util.now();
      const [code, value] = await verify(root, key, proof);
      assert(code === 0);
      assert(value);
      console.log('Verify: %d.', util.now() - now);
    }
  }

  return tree.close();
}

(async () => {
  const arg = argv.length >= 3
    ? argv[2]
    : '';

  switch (arg) {
    case 'stress':
      console.log('Stress testing...');
      return stress(FILE);
    case 'bench':
      console.log('Benchmarking (disk)...');
      return bench(FILE);
    case 'benchVTX':
      console.log('Benchmarking VTX (disk)...');
      return benchVTX(FILE);
    default:
      console.log('Choose either stress, bench or benchVTX...');
      return null;
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
