/**
 * tree.h - tree API bindings.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#ifndef _NURKEL_TREE_H
#define _NURKEL_TREE_H

#include <stdbool.h>
#include <stdint.h>
#include <urkel.h>
#include <node_api.h>
#include "common.h"
#include "util.h"

/*
 * Tree helper macros.
 */

#define NURKEL_TREE_CONTEXT()                                                  \
  nurkel_tree_t *ntree = NULL;                                                 \
  JS_ASSERT(napi_get_value_external(env, argv[0], (void **)&ntree) == napi_ok, \
            JS_ERR_ARG);                                                       \
  JS_ASSERT(ntree != NULL, JS_ERR_ARG)

#define NURKEL_TREE_READY() do {                               \
  enum nurkel_state_err tree_state = nurkel_tree_ready(ntree); \
  if (tree_state != nurkel_state_err_ok)                       \
    JS_THROW(state_errors[tree_state]);                        \
} while(0)

/*
 * Tree methods.
 */

napi_status
nurkel_final_check(napi_env env, nurkel_tree_t *ntree);

/* Exported for the transaction. */
enum nurkel_state_err
nurkel_tree_ready(nurkel_tree_t *ntree);

/* Exported for the transaction. */
void
nurkel_register_tx(struct nurkel_tx_s *ntx);

/* Exported for the transaction. */
void
nurkel_unregister_tx(struct nurkel_tx_s *ntx);

/* Imported from the transaction. */
napi_status
nurkel_tx_queue_close_worker(napi_env env, nurkel_tx_t *ntx, napi_deferred deferred);

/* Imported from the transaction. */
napi_status
nurkel_tx_final_check(napi_env env, nurkel_tx_t *ntx);

/*
 * Tree binding declarations.
 */

NURKEL_METHOD(tree_init);
NURKEL_METHOD(tree_open);
NURKEL_METHOD(tree_close);
NURKEL_METHOD(tree_root_hash_sync);
NURKEL_METHOD(tree_root_hash);
NURKEL_METHOD(tree_inject_sync);
NURKEL_METHOD(tree_inject);
NURKEL_METHOD(tree_get_sync);
NURKEL_METHOD(tree_get);
NURKEL_METHOD(tree_has_sync);
NURKEL_METHOD(tree_has);
NURKEL_METHOD(tree_insert_sync);
NURKEL_METHOD(tree_insert);
NURKEL_METHOD(tree_remove_sync);
NURKEL_METHOD(tree_remove);
NURKEL_METHOD(tree_prove_sync);
NURKEL_METHOD(tree_prove);
NURKEL_METHOD(verify_sync);
NURKEL_METHOD(verify);
NURKEL_METHOD(destroy_sync);
NURKEL_METHOD(destroy);
NURKEL_METHOD(hash_sync);
NURKEL_METHOD(hash);
NURKEL_METHOD(compact_sync);
NURKEL_METHOD(compact);
NURKEL_METHOD(stat_sync);
NURKEL_METHOD(stat);

#endif /* _NURKEL_TREE_H */
