'use strict';

const path = require('path');
const assert = require('bsert');
const fs = require('fs');
const {testdir, rmTreeDir, isTreeDir} = require('./util/common');
const {Tree} = require('../lib/tree');

describe('Urkel Open (GC)', function () {
  if (!global.gc)
    this.skip();

  let prefix, treeDir;

  beforeEach(() => {
    prefix = testdir('open');
    treeDir = path.join(prefix, 'tree');
    fs.mkdirSync(prefix);
  });

  afterEach(() => {
    if (fs.existsSync(treeDir))
      rmTreeDir(treeDir);

    fs.rmdirSync(prefix);
  });

  it('should open and close database', async () => {
    {
      const tree = new Tree({prefix});

      await tree.open();
      await tree.close();
    }

    assert(isTreeDir(treeDir), 'Tree was not created.');
    global.gc();
  });

  it('should cleanup', async () => {
    {
      const tree = new Tree({prefix});
      tree.init();
    }

    global.gc();
  });

  it('should open, close and cleanup', async () => {
    {
      const tree = new Tree({prefix});

      await tree.open();
      await tree.close();
    }

    assert(isTreeDir(treeDir));
    global.gc();
  });

  it('should open and cleanup', async () => {
    {
      const tree = new Tree({prefix});

      await tree.open();
    };

    assert(isTreeDir(treeDir, true));
    global.gc();
  });
});
