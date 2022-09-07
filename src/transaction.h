/**
 * transaction.h - transaction API bindings.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#ifndef _NURKEL_TX_H
#define _NURKEL_TX_H

#include <stdbool.h>
#include <stdint.h>
#include <urkel.h>
#include <node_api.h>
#include "common.h"
#include "util.h"
#include "tree.h"

/*
 * Helper macros for TXs.s
 */

#define NURKEL_TX_CONTEXT()                                                  \
  nurkel_tx_t *ntx = NULL;                                                   \
  nurkel_tree_t *ntree = NULL;                                               \
  JS_ASSERT(napi_get_value_external(env, argv[0], (void **)&ntx) == napi_ok, \
            JS_ERR_ARG);                                                     \
  JS_ASSERT(ntx != NULL, JS_ERR_ARG);                                        \
  ntree = ntx->ntree

#define NURKEL_TX_READY() do {                             \
  enum inst_state tree_state = nurkel_tree_ready(ntree);   \
  if (tree_state != inst_state_ok)                         \
    JS_THROW(inst_errors[tree_state]);                     \
                                                           \
  enum inst_state tx_state = nurkel_tx_ready(ntx);         \
  if (tx_state != inst_state_ok)                           \
    JS_THROW(inst_errors[tx_state]);                       \
} while(0)

/*
 * Transaction workers
 */

typedef struct nurkel_tx_open_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
} nurkel_tx_open_worker_t;

typedef struct nurkel_tx_close_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  bool in_destroy;
} nurkel_tx_close_worker_t;

typedef struct nurkel_tx_root_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  uint8_t out_hash[URKEL_HASH_SIZE];
} nurkel_tx_root_hash_worker_t;

typedef struct nurkel_tx_get_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  uint8_t in_key[URKEL_HASH_SIZE];

  uint8_t out_value[URKEL_VALUE_SIZE];
  size_t out_value_len;
  bool out_has_key;
} nurkel_tx_get_worker_t;

typedef struct nurkel_tx_has_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  uint8_t in_key[URKEL_HASH_SIZE];

  bool out_has_key;
} nurkel_tx_has_worker_t;

typedef struct nurkel_tx_insert_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  uint8_t in_key[URKEL_HASH_SIZE];
  uint8_t in_value[URKEL_VALUE_SIZE];
  size_t in_value_len;
} nurkel_tx_insert_worker_t;

typedef struct nurkel_tx_remove_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  uint8_t in_key[URKEL_HASH_SIZE];
} nurkel_tx_remove_worker_t;

typedef struct nurkel_tx_prove_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  uint8_t in_key[URKEL_HASH_SIZE];
  uint8_t *out_proof;
  size_t out_proof_len;
} nurkel_tx_prove_worker_t;

typedef struct nurkel_tx_commit_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  uint8_t out_hash[URKEL_HASH_SIZE];
} nurkel_tx_commit_worker_t;

typedef struct nurkel_tx_clear_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
} nurkel_tx_clear_worker_t;

typedef struct nurkel_tx_inject_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  uint8_t in_root[URKEL_HASH_SIZE];
} nurkel_tx_inject_worker_t;

/*
 * Transaction life cycle methods.
 */

void
nurkel_ntx_init(nurkel_tx_t *ntx);

enum inst_state
nurkel_tx_ready(nurkel_tx_t *ntx);

napi_status
nurkel_tx_close_try_close(napi_env env, nurkel_tx_t *ntx);

void
nurkel_tx_destroy(napi_env env, void *data, void *hint);

napi_status
nurkel_tx_close_work(nurkel_tx_close_params_t params);

/*
 * Transaction bindings.
 */

NURKEL_METHOD(tx_init);
NURKEL_METHOD(tx_close);
NURKEL_METHOD(tx_init);
NURKEL_METHOD(tx_open);
NURKEL_METHOD(tx_close);
NURKEL_METHOD(tx_root_hash_sync);
NURKEL_METHOD(tx_root_hash);
NURKEL_METHOD(tx_get_sync);
NURKEL_METHOD(tx_get);
NURKEL_METHOD(tx_has_sync);
NURKEL_METHOD(tx_has);
NURKEL_METHOD(tx_insert_sync);
NURKEL_METHOD(tx_insert);
NURKEL_METHOD(tx_remove_sync);
NURKEL_METHOD(tx_remove);
NURKEL_METHOD(tx_prove_sync);
NURKEL_METHOD(tx_prove);
NURKEL_METHOD(tx_commit_sync);
NURKEL_METHOD(tx_commit);
NURKEL_METHOD(tx_clear_sync);
NURKEL_METHOD(tx_clear);
NURKEL_METHOD(tx_inject_sync);
NURKEL_METHOD(tx_inject);

#endif /* _NURKEL_TX_H */
