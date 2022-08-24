'use strict';

const common = require('./common');

const nurkel = exports;

const definePath = (name, path) => {
  let cache = null;

  Object.defineProperty(nurkel, name, {
    get() {
      if (!cache)
        cache = require(path);

      return cache;
    }
  });
};

const defineGet = (name, get) => {
  Object.defineProperty(nurkel, name, { get });
};

definePath('_disk', './tree');
definePath('_memory', './memory');
definePath('BLAKE2b', './blake2b');

defineGet('Tree', () => nurkel._disk.Tree);
defineGet('MemTree', () => nurkel._memory.Tree);
defineGet('UrkelTree', () => nurkel._memory.Tree);

nurkel.common = common;
nurkel.proofCodes = common.proofCodes;
nurkel.proofCodesByVal = common.proofCodesByVal;

nurkel.create = (options) => {
  if (options.memory)
    return new nurkel.MemTree();

  return new nurkel.Tree({
    prefix: options.prefix
  });
};
