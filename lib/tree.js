'use strict';

const assert = require('bsert');
const path = require('path');
const nurkel = require('loady')('nurkel', __dirname);

const ERR_NOT_IMPL = 'Not implemented.';
const ERR_INIT = 'Database has already been initialized.';
const ERR_NO_INIT = 'Database has not been initialized.';
const ERR_OPEN = 'Database is already open.';
const ERR_CLOSED = 'Database is already closed.';

class Tree {
  constructor(options) {
    this.options = new TreeOptions(options);
    this.prefix = this.options.prefix;
    this.path = path.join(this.prefix, 'tree');

    this.isOpen = false;
    this.db = null;
  }

  get bits() {
    return 256;
  }

  get hashSize() {
    return 32;
  }

  get keySize() {
    return 32;
  }

  isHash(hash) {
    if (!Buffer.isBuffer(hash))
      return false;
    return hash.length === this.size;
  }

  isKey(key) {
    if (!Buffer.isBuffer(key))
      return false;

    // TODO: Double check
    return key.length === (this.bits >>> 3);
  }

  async open(root) {
    assert(!this.db, ERR_INIT);
    assert(!this.isOpen, ERR_OPEN);
    assert(!root, ERR_NOT_IMPL);

    this.db = nurkel.init();

    await nurkel.open(this.db, this.path);

    return this;
  }

  async close() {
    assert(this.db, ERR_NO_INIT);
    assert(this.isOpen, ERR_CLOSED);

    await nurkel.close(this.db);
    return this;
  }
}

class TreeOptions {
  constructor(options) {
    this.prefix = '/';

    this.fromOptions(options);
  }

  fromOptions(options) {
    assert(options, 'Tree requires options.');
    assert(typeof options.prefix === 'string',
      'options.prefix must be a string.');

    this.prefix = options.prefix;
  }
}

exports.Tree = Tree;
