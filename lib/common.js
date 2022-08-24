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
 * @enum {Number}
 */

const proofCodes = {
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

const proofCodesByVal = [
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

exports.errors = errors;
exports.proofCodes = proofCodes;
exports.proofCodesByVal = proofCodesByVal;
exports.asyncIterator = asyncIterator;
