'use strict';

const assert = require('bsert');
const {Tree, BLAKE2b} = require('..');
const vectors = require('./data/blake2b.json');

describe('BLAKE2b', function() {
  describe('Tree.hash & Tree.hashSync', function() {
    for (const [hexmsg, size, hexkey, hexexpect] of vectors) {
      const text = hexexpect.slice(0, 32) + '...';
      const msg = Buffer.from(hexmsg, 'hex');
      const key = Buffer.from(hexkey, 'hex');
      const expect = Buffer.from(hexexpect, 'hex');

      if (size !== 32 || key.length !== 0)
        continue;

      it(`should get hash of ${text}`, async () => {
        const m = Buffer.from(msg, 'hex');
        const e = Buffer.from(expect, 'hex');

        const hash = await Tree.hash(m);
        const hashSync = Tree.hashSync(m);

        assert.bufferEqual(hash, e);
        assert.bufferEqual(hashSync, e);
      });
    }
  });

  describe('Simple BLAKE2b wrapper', function () {
    for (const [hexmsg, size, hexkey, hexexpect] of vectors) {
      const text = hexexpect.slice(0, 32) + '...';
      const msg = Buffer.from(hexmsg, 'hex');
      const key = Buffer.from(hexkey, 'hex');
      const expect = Buffer.from(hexexpect, 'hex');

      it(`should get hash using .digest of ${text}`, async () => {
        const m = Buffer.from(msg, 'hex');
        const e = Buffer.from(expect, 'hex');

        const hash = BLAKE2b.digest(m, size, key);

        assert.bufferEqual(hash, e);
      });

      it(`should get hash using .update of ${text}`, async () => {
        const m = Buffer.from(msg, 'hex');
        const e = Buffer.from(expect, 'hex');

        const parts = [];

        for (let i = 0; i < m.length; i++)
          parts.push(m.slice(i, i + 1));

        const ctx = BLAKE2b.hash().init(size, key);
        for (const part of parts)
          ctx.update(part);

        const hash = ctx.final();

        assert.bufferEqual(hash, e);
      });

      it(`should get hash using .multi of ${text}`, async () => {
        const m = Buffer.from(msg, 'hex');
        const e = Buffer.from(expect, 'hex');

        if (m.length < 3)
          this.skip();

        const x = m.slice(0, m.length / 3);
        const y = m.slice(m.length / 3, 2 * m.length / 3);
        const z = m.slice(2 * m.length / 3);

        const hash = BLAKE2b.multi(x, y, z, size, key);

        assert.bufferEqual(hash, e);
      });
    }
  });
});

