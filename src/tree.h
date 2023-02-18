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

#define NURKEL_TREE_READY() do {                         \
  enum inst_state tree_state = nurkel_tree_ready(ntree); \
  if (tree_state != inst_state_ok)                       \
    JS_THROW(inst_errors[tree_state]);                   \
} while(0)

/*
 * Tree workers
 */

typedef struct nurkel_open_worker_s {
  WORKER_BASE_PROPS(nurkel_tree_t)
  char *in_path;
  size_t in_path_len;

  uint8_t out_hash[URKEL_HASH_SIZE];
} nurkel_open_worker_t;

typedef struct nurkel_close_worker_s {
  WORKER_BASE_PROPS(nurkel_tree_t)
  bool in_destroy;
} nurkel_close_worker_t;

typedef struct nurkel_destroy_worker_s {
  WORKER_BASE_PROPS(nurkel_tree_t)
  char *in_path;
  size_t in_path_len;
} nurkel_destroy_worker_t;

typedef struct nurkel_root_hash_worker_s {
  WORKER_BASE_PROPS(nurkel_tree_t)
  uint8_t out_hash[URKEL_HASH_SIZE];
} nurkel_root_hash_worker_t;

typedef struct nurkel_get_worker_s {
  WORKER_BASE_PROPS(nurkel_tree_t)
  uint8_t in_key[URKEL_HASH_SIZE];

  uint8_t out_value[URKEL_VALUE_SIZE];
  size_t out_value_len;
  bool out_has_key;
} nurkel_get_worker_t;

typedef struct nurkel_inject_worker_s {
  WORKER_BASE_PROPS(nurkel_tree_t)
  uint8_t in_root[URKEL_HASH_SIZE];
} nurkel_inject_worker_t;

typedef struct nurkel_hash_worker_s {
  WORKER_BASE_PROPS(nurkel_tree_t)
  uint8_t *in_data;
  size_t in_data_len;

  uint8_t out_hash[URKEL_HASH_SIZE];
} nurkel_hash_worker_t;

typedef struct nurkel_has_worker_s {
  WORKER_BASE_PROPS(nurkel_tree_t)
  uint8_t in_key[URKEL_HASH_SIZE];

  bool out_has_key;
} nurkel_has_worker_t;

typedef struct nurkel_prove_worker_s {
  WORKER_BASE_PROPS(nurkel_tree_t)
  uint8_t in_key[URKEL_HASH_SIZE];

  uint8_t *out_proof;
  size_t out_proof_len;
} nurkel_prove_worker_t;

typedef struct nurkel_verify_worker_s {
  WORKER_BASE_PROPS(void)
  uint8_t in_root[URKEL_HASH_SIZE];
  uint8_t in_key[URKEL_HASH_SIZE];
  uint8_t *in_proof;
  size_t in_proof_len;

  int out_exists;
  uint8_t out_value[URKEL_VALUE_SIZE];
  size_t out_value_len;
} nurkel_verify_worker_t;

typedef struct nurkel_compact_worker_s {
  WORKER_BASE_PROPS(void)
  char *in_src;
  size_t in_src_len;
  char *in_dst;
  size_t in_dst_len;
  uint8_t *in_root;
} nurkel_compact_worker_t;

typedef struct nurkel_stat_worker_s {
  WORKER_BASE_PROPS(void)
  char *in_prefix;
  size_t in_prefix_len;
  urkel_tree_stat_t out_st;
} nurkel_stat_worker_t;

/*
 * Tree life cycle management.
 */

void
nurkel_ntree_init(nurkel_tree_t *ntree);

enum inst_state
nurkel_tree_ready(nurkel_tree_t *ntree);

napi_status
nurkel_close_try_close(napi_env env, nurkel_tree_t *ntree);

void
nurkel_register_tx(struct nurkel_tx_s *ntx);

void
nurkel_unregister_tx(struct nurkel_tx_s *ntx);

napi_status
nurkel_tx_queue_close_worker(napi_env env, nurkel_tx_t *ntx, napi_deferred deferred);

napi_status
nurkel_tx_final_check(napi_env env, nurkel_tx_t *ntx);

void
nurkel_ntree_destroy(napi_env env, void *data, void *hint);

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
