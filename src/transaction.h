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

extern const char *txn_state_errors[];
extern const char *iter_state_errors[];

#define VTX_OP_INSERT 1
#define VTX_OP_REMOVE 2

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

#define NURKEL_TX_READY() do {                                   \
  enum nurkel_state_err tree_state = nurkel_ntree_ready(ntree);  \
  if (tree_state != nurkel_state_err_ok)                         \
    JS_THROW(tree_state_errors[tree_state]);                     \
                                                                 \
  enum nurkel_state_err tx_state = nurkel_ntx_ready(ntx);        \
  if (tx_state != nurkel_state_err_ok)                           \
    JS_THROW(txn_state_errors[tx_state]);                        \
} while(0)

#define NURKEL_ITER_CONTEXT()                                                  \
  nurkel_iter_t *niter = NULL;                                                 \
  nurkel_tx_t *ntx = NULL;                                                     \
  nurkel_tree_t *ntree = NULL;                                                 \
  JS_ASSERT(napi_get_value_external(env, argv[0], (void **)&niter) == napi_ok, \
            JS_ERR_ARG);                                                       \
  JS_ASSERT(niter != NULL, JS_ERR_ARG);                                        \
  ntx = niter->ntx;                                                            \
  ntree = ntx->ntree

#define NURKEL_ITER_READY() do {                                   \
  enum nurkel_state_err tree_state = nurkel_ntree_ready(ntree);    \
  if (tree_state != nurkel_state_err_ok)                           \
    JS_THROW(tree_state_errors[tree_state]);                       \
                                                                   \
  enum nurkel_state_err ntx_state = nurkel_ntx_ready(ntx);         \
  if (ntx_state != nurkel_state_err_ok)                            \
    JS_THROW(txn_state_errors[ntx_state]);                         \
                                                                   \
  enum nurkel_state_err niter_state = nurkel_niter_ready(niter);   \
  if (niter_state != nurkel_state_err_ok)                          \
    JS_THROW(iter_state_errors[niter_state]);                      \
} while(0)

/*
 * Transaction life cycle methods.
 */

napi_status
nurkel_tx_queue_close_worker(napi_env env,
                             nurkel_tx_t *ntx,
                             napi_deferred deferred);

napi_status
nurkel_tx_final_check(napi_env env, nurkel_tx_t *ntx);

napi_status
nurkel_tx_debug_info(napi_env,
                     nurkel_tx_t *ntx,
                     napi_value object,
                     bool expand);

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
NURKEL_METHOD(tx_apply);
NURKEL_METHOD(tx_apply_sync);

/*
 * Iterator bindings.
 */
NURKEL_METHOD(iter_init);
NURKEL_METHOD(iter_close);
NURKEL_METHOD(iter_next_sync);
NURKEL_METHOD(iter_next);

#endif /* _NURKEL_TX_H */
