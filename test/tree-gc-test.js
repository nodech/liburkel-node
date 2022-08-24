'use strict';

const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir, isTreeDir} = require('./util/common');
const {Tree} = require('..');

describe('Urkel Tree (GC)', function () {
  if (!global.gc)
    this.skip();

  let prefix;

  beforeEach(() => {
    prefix = testdir('tree-gc');
    fs.mkdirSync(prefix);
  });

  afterEach(() => {
    if (isTreeDir(prefix))
      rmTreeDir(prefix);
  });

  it('should open and close database', async () => {
    {
      const tree = new Tree({prefix});

      await tree.open();
      await tree.close();
    }

    assert(isTreeDir(prefix), 'Tree was not created.');
    global.gc();
  });

  it('should cleanup', async () => {
    {
      new Tree({prefix});
    }

    global.gc();
  });

  it('should open, close and cleanup', async () => {
    {
      const tree = new Tree({prefix});

      await tree.open();
      await tree.close();
    }

    assert(isTreeDir(prefix));
    global.gc();
  });

  it('should open and cleanup', async () => {
    {
      const tree = new Tree({prefix});

      await tree.open();
    };

    assert(isTreeDir(prefix, true));
    global.gc();
  });
});
