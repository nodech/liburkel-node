'use strict';

const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir, isTreeDir} = require('./util/common');
const nurkel = require('..');
const {BLAKE2b, proofTypes, statusCodes} = nurkel;

const foo = n => BLAKE2b.digest(Buffer.from('foo' + n));
const bar = n => Buffer.from('bar' + n);

async function populateTree(tree, roots) {
  let entries = [];
  let txn = null;

  const insert = async (key, value) => {
    entries.push({ key, value });
    await txn.insert(key, value);
  };

  const commit = async () => {
    const root = await txn.commit();
    roots[root.toString('hex')] = entries;
    entries = [];
  };

  roots[tree.rootHash().toString('hex')] = [];
  txn = tree.txn();
  await txn.open();

  await insert(foo(1), bar(1));
  await insert(foo(2), bar(2));
  await commit();

  await insert(foo(3), bar(3));
  await insert(foo(4), bar(4));
  await commit();
  await txn.close();
}

for (const memory of [false, true]) {
describe(`Urkel Proof (${memory ? 'MemTree' : 'Tree'})`, function () {
  const Tree = memory ? nurkel.MemTree : nurkel.Tree;
  let prefix, tree, rootEntries, checkMatrix;

  const withSnap = (snap) => {
    return {
      ...checkMatrix,
      'snap.verify': (_, key, proof) => snap.verify(key, proof),
      'snap.verifySync': (_, key, proof) => snap.verifySync(key, proof)
    };
  };

  beforeEach(async () => {
    prefix = testdir('tree');

    if (!memory)
      fs.mkdirSync(prefix);

    tree = nurkel.create({ memory, prefix });
    await tree.open();
    rootEntries = {};
    await populateTree(tree, rootEntries);

    checkMatrix = {
      'Tree.verify': Tree.verify,
      'Tree.verifySync': Tree.verifySync,
      'tree.verify': tree.verify.bind(tree),
      'tree.verifySync': tree.verifySync.bind(tree)
    };
  });

  afterEach(async () => {
    await tree.close();

    if (!memory && isTreeDir(prefix))
      rmTreeDir(prefix);
  });

  it('should get proof of existence (tree)', async () => {
    const proofs = await Promise.all(
      Object.values(rootEntries).flat().map(async ({key, value}) => {
        return {key, value, proof: await tree.prove(key)};
      })
    );

    for (const {key, value, proof} of proofs) {
      assert.strictEqual(proof.type, proofTypes.TYPE_EXISTS);

      const root = tree.rootHash();
      const modifiedKey = Buffer.concat([key]);
      modifiedKey[31] = 0x00;

      for (const [name, fn] of Object.entries(checkMatrix)) {
        let [rcode, rvalue] = await fn(root, key, proof);
        assert.strictEqual(rcode, statusCodes.URKEL_OK, `Error in ${name}.`);
        assert.bufferEqual(rvalue, value, `Error check value in ${name}.`);

        [rcode, rvalue] = await fn(root, modifiedKey, proof);
        assert.strictEqual(rcode, statusCodes.URKEL_EHASHMISMATCH,
                           `Error in ${name}.`);
        assert.strictEqual(rvalue, null, `Error check value in ${name}.`);

        [rcode, rvalue] = await fn(root, foo(5), proof);
        assert.strictEqual(rcode, statusCodes.URKEL_EPATHMISMATCH,
                           `Error in ${name}.`);
        assert.strictEqual(rvalue, null, `Error check value in ${name}.`);
      }
    }
  });

  it('should get proof of existence (tx)', async () => {
    const snap = tree.snapshot();
    await snap.open();
    const allProofs = await Promise.all(
      Object.values(rootEntries).flat().map(async ({ key, value }) => {
        return {key, value, proof: await snap.prove(key)};
      })
    );
    const checkOK = async ({name, fn, root, key, value, proof}) => {
      let [rcode, rvalue] = await fn(root, key, proof);
      assert.strictEqual(rcode, statusCodes.URKEL_OK, `Error in ${name}.`);
      assert.bufferEqual(rvalue, value, `Error check value in ${name}.`);

      const modifiedKey = Buffer.concat([key]);
      modifiedKey[31] = 0x00;
      [rcode, rvalue] = await fn(root, modifiedKey, proof);
      assert.strictEqual(rcode, statusCodes.URKEL_EHASHMISMATCH,
                         `Error in ${name}.`);
      assert.strictEqual(rvalue, null, `Error check value in ${name}.`);
    };

    for (const {key, value, proof} of allProofs) {
      assert.strictEqual(proof.type, proofTypes.TYPE_EXISTS);
      const root = snap.rootHash();

      for (const [name, fn] of Object.entries(checkMatrix))
        await checkOK({ key, value, root, proof, name, fn });
    }

    await snap.close();

    for (const [rootHex, entries] of Object.entries(rootEntries)) {
      const root = Buffer.from(rootHex, 'hex');
      const snap = tree.snapshot(root);
      const withSnaps = withSnap(snap);
      await snap.open();

      const proofs = await Promise.all(
        Object.values(entries).flat().map(async ({ key, value }) => {
          return {key, value, proof: await snap.prove(key)};
        })
      );

      for (const {key, value, proof} of proofs) {
        assert.strictEqual(proof.type, proofTypes.TYPE_EXISTS);
        const modifiedKey = Buffer.concat([key]);
        modifiedKey[31] = 0x00;

        for (const [name, fn] of Object.entries(withSnaps))
          await checkOK({ key, value, root, proof, name, fn });
      }

      await snap.close();
    }
  });

  it('should get proof of nonexistence/DEADEND (tx)', async () => {
    const root = Buffer.alloc(32, 0x00);
    const snap = await tree.snapshot(root);
    await snap.open();
    const proof = await snap.prove(foo(3));
    assert.strictEqual(proof.type, proofTypes.TYPE_DEADEND);
    await snap.close();

    for (const [name, fn] of Object.entries(withSnap(snap))) {
      const [rcode, rvalue] = await fn(root, foo(3), proof);
      assert.strictEqual(rcode, statusCodes.URKEL_OK, `Error in ${name}.`);
      assert.strictEqual(rvalue, null, `Error check value in ${name}.`);
    }
  });

  it('should get proof of nonexistence/TYPE_SHORT', async () => {
    const proofs = [];
    let proof = await tree.prove(foo(6));
    assert.strictEqual(proof.type, proofTypes.TYPE_SHORT);
    proofs.push(proof);

    const snap = tree.snapshot();
    await snap.open();
    proof = await snap.prove(foo(6));
    assert.strictEqual(proof.type, proofTypes.TYPE_SHORT);
    proofs.push(proof);

    for (const proof of proofs) {
      const root = tree.rootHash();

      for (const [name, fn] of Object.entries(withSnap(snap))) {
        const [rcode, rvalue] = await fn(root, foo(6), proof);
        assert.strictEqual(rcode, statusCodes.URKEL_OK, `Error in ${name}.`);
        assert.strictEqual(rvalue, null, `Error check value in ${name}.`);
      }
    }

    await snap.close();
  });

  it('should get proof of nonexistence/TYPE_COLLISION', async () => {
    // duplicate buffer.
    const modifiedFOO1 = foo(1);
    modifiedFOO1[31] = 0x00;

    const proofs = [];
    let proof = await tree.prove(modifiedFOO1);
    assert.strictEqual(proof.type, proofTypes.TYPE_COLLISION);
    proofs.push(proof);

    const snap = tree.snapshot();
    await snap.open();
    proof = await snap.prove(modifiedFOO1);
    assert.strictEqual(proof.type, proofTypes.TYPE_COLLISION);
    proofs.push(proof);

    for (const proof of proofs) {
      const root = tree.rootHash();

      for (const [name, fn] of Object.entries(withSnap(snap))) {
        let [rcode, rvalue] = await fn(root, modifiedFOO1, proof);
        assert.strictEqual(rcode, statusCodes.URKEL_OK, `Error in ${name}.`);
        assert.strictEqual(rvalue, null, `Error check value in ${name}.`);

        [rcode, rvalue] = await fn(root, foo(1), proof);
        assert.strictEqual(rcode, statusCodes.URKEL_ESAMEKEY,
                           `Error in ${name}.`);
        assert.strictEqual(rvalue, null,  `Error check value in ${name}.`);
      }
    }

    await snap.close();
  });
});
}
