/**
 * common.h - common things to nurkel.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#include "common.h"

const char *
urkel_errors[] = {
  "URKEL_OK",
  "URKEL_EHASHMISMATCH",
  "URKEL_ESAMEKEY",
  "URKEL_ESAMEPATH",
  "URKEL_ENEGDEPTH",
  "URKEL_EPATHMISMATCH",
  "URKEL_ETOODEEP",
  "URKEL_EINVAL",
  "URKEL_ENOTFOUND",
  "URKEL_ECORRUPTION",
  "URKEL_ENOUPDATE",
  "URKEL_EBADWRITE",
  "URKEL_EBADOPEN",
  "URKEL_EITEREND"
};

const int urkel_errors_len = sizeof(urkel_errors) / sizeof(urkel_errors[0]);

const char *inst_errors[] = {
  "ok.",
  "is closed.",
  "is closing.",
  "is opening.",
  "should close"
};
