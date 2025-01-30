'use strict';

const common = require('./common');
const Proof = require('./proof');

/** @typedef {import('./tree')} Nurkel */
/** @typedef {import('./tree')} Urkel */

/** @typedef {import('./tree').Tree} Tree */
/** @typedef {import('./urkel').Tree} UrkelTree */

/**
 * @type {Object}
 * @property {Nurkel} _disk
 * @property {Urkel} _memory
 * @property {Urkel} _urkel
 * @property {Tree} Tree
 * @property {UrkelTree} MemTree
 * @property {UrkelTree} UrkelTree
 */

const nurkel = exports;

/**
 * @param {String} name
 * @param {String} path
 * @returns {void}
 */

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

/**
 * @param {String} name
 * @param {Function} get
 * @returns {void}
 */

const defineGet = (name, get) => {
  Object.defineProperty(nurkel, name, { get });
};

definePath('_disk', './tree');
definePath('_urkel', './urkel');
definePath('_memory', './urkel');
definePath('BLAKE2b', './blake2b');

defineGet('Tree', () => nurkel._disk.Tree);
defineGet('MemTree', () => nurkel._memory.Tree);
defineGet('UrkelTree', () => nurkel._urkel.Tree);

/**
 * @param {Object} options
 * @param {Boolean} [options.memory=false] - should use memory tree.
 * @param {Boolean} [options.urkel] - should use urkel tree.
 * @param {String} [options.prefix] - prefix for the database.
 * @returns {Tree|UrkelTree}
 */

nurkel.create = (options) => {
  if (options.memory)
    return new nurkel.MemTree();

  if (options.urkel) {
    return new nurkel.UrkelTree({
      prefix: options.prefix
    });
  }

  return new nurkel.Tree({
    prefix: options.prefix
  });
};

nurkel.Proof = Proof;
nurkel.common = common;
nurkel.statusCodes = common.statusCodes;
nurkel.statusCodesByVal = common.statusCodesByVal;
nurkel.proofTypes = common.proofTypes;
nurkel.proofTypesByVal = common.proofTypesByVal;
