'use strict';

const assert = require('bsert');
const {Tree} = require('../lib/tree');
const vectors = require('./data/blake2b-urkel.json');

describe('BLAKE2b', function() {
  for (const [msg, , , expect] of vectors) {
    const text = expect.slice(0, 32) + '...';

    it(`should get BLAKE2b hash of ${text}`, async () => {
      const m = Buffer.from(msg, 'hex');
      const e = Buffer.from(expect, 'hex');

      const hash = await Tree.hash(m);
      const hashSync = Tree.hashSync(m);

      assert.bufferEqual(hash, e);
      assert.bufferEqual(hashSync, e);
    });
  }
});

