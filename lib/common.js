/**
 * nurkel.c - common things
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

'use strict';

/*
 * Compat
 */

const asyncIterator = Symbol.asyncIterator || 'asyncIterator';
const syncIterator = Symbol.iterator || 'iterator';

/*
 * General errors
 */

const errors = {
  // General errors
  ERR_PLACEHOLDER: 'Placeholder method.', // wont implement.
  ERR_NOT_IMPL: 'Not implemented.',
  ERR_NOT_SUPPORTED: 'Not supported.',

  // Database state errors
  ERR_INIT: 'Database has already been initialized.',
  ERR_NOT_INIT: 'Database has not been initialized.',
  ERR_OPEN: 'Database is already open.',
  ERR_NOT_OPEN: 'Database is not open.',
  ERR_CLOSED: 'Database is already closed.',

  // Transaction state errors
  ERR_TX_OPEN: 'Transaction is already open.',
  ERR_TX_NOT_OPEN: 'Transaction is not open.',
  ERR_TX_NOT_FLUSHED: 'Transaction is not flushed.'
};

/*
 * Proof constants
 */

const URKEL_OK = 0;
const URKEL_EHASHMISMATCH = 1;
const URKEL_ESAMEKEY = 2;
const URKEL_ESAMEPATH = 3;
const URKEL_ENEGDEPTH = 4;
const URKEL_EPATHMISMATCH = 5;
const URKEL_ETOODEEP = 6;
const URKEL_EINVAL = 7;
const URKEL_ENOTFOUND = 8;
const URKEL_ECORRUPTION = 9;
const URKEL_ENOUPDATE = 10;
const URKEL_EBADWRITE = 11;
const URKEL_EBADOPEN = 12;
const URKEL_EITEREND = 13;

/**
 * Verification error codes.
 * @global
 * @enum {NurkelStatus}
 */

const statusCodes = {
  URKEL_OK,
  URKEL_EHASHMISMATCH,
  URKEL_ESAMEKEY,
  URKEL_ESAMEPATH,
  URKEL_ENEGDEPTH,
  URKEL_EPATHMISMATCH,
  URKEL_ETOODEEP,
  URKEL_EINVAL,
  URKEL_ENOTFOUND,
  URKEL_ECORRUPTION,
  URKEL_ENOUPDATE,
  URKEL_EBADWRITE,
  URKEL_EBADOPEN,
  URKEL_EITEREND
};

const statusCodesByVal = [
  'URKEL_OK',
  'URKEL_EHASHMISMATCH',
  'URKEL_ESAMEKEY',
  'URKEL_ESAMEPATH',
  'URKEL_ENEGDEPTH',
  'URKEL_EPATHMISMATCH',
  'URKEL_ETOODEEP',
  'URKEL_EINVAL',
  'URKEL_ENOTFOUND',
  'URKEL_ECORRUPTION',
  'URKEL_ENOUPDATE',
  'URKEL_EBADWRITE',
  'URKEL_EBADOPEN',
  'URKEL_EITEREND'
];

const TYPE_DEADEND = 0;
const TYPE_SHORT = 1;
const TYPE_COLLISION = 2;
const TYPE_EXISTS = 3;
const TYPE_UNKNOWN = 4;

/**
 * Proof types.
 * @enum {ProofType}
 */

const proofTypes = {
  TYPE_DEADEND,
  TYPE_SHORT,
  TYPE_COLLISION,
  TYPE_EXISTS,
  TYPE_UNKNOWN
};

/**
 * Proof types (strings).
 * @const {String[]}
 * @default
 */

const proofTypesByVal = [
  'TYPE_DEADEND',
  'TYPE_SHORT',
  'TYPE_COLLISION',
  'TYPE_EXISTS',
  'TYPE_UNKNOWN'
];

/*
 * Iterator
 */

const ITER_TYPE_KEYS = 0;
const ITER_TYPE_VALUES = 1;
const ITER_TYPE_KEY_VAL = 2;

/**
 * Iteration types.
 * @enum {IteratorType}
 */

const iteratorTypes = {
  ITER_TYPE_KEYS,
  ITER_TYPE_VALUES,
  ITER_TYPE_KEY_VAL
};

/**
 * Iterator types (strings).
 * @const {String[]}
 */

const iteratorTypesByVal = [
  'ITER_TYPE_KEYS',
  'ITER_TYPE_VALUES',
  'ITER_TYPE_KEY_VAL'
];

exports.asyncIterator = asyncIterator;
exports.syncIterator = syncIterator;
exports.errors = errors;
exports.statusCodes = statusCodes;
exports.statusCodesByVal = statusCodesByVal;
exports.proofTypes = proofTypes;
exports.proofTypesByVal = proofTypesByVal;
exports.iteratorTypes = iteratorTypes;
exports.iteratorTypesByVal = iteratorTypesByVal;
