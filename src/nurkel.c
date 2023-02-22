/**
 * nurkel.c - native bindings to liburkel.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <urkel.h>
#include <node_api.h>

#include "common.h"
#include "util.h"
#include "tree.h"
#include "transaction.h"

/*
 * Module
 */

#ifndef NAPI_MODULE_INIT
#define NAPI_MODULE_INIT()                                       \
static napi_value nurkel_init(napi_env env, napi_value exports); \
NAPI_MODULE(NODE_GYP_MODULE_NAME, nurkel_init)                   \
static napi_value nurkel_init(napi_env env, napi_value exports)
#endif

NAPI_MODULE_INIT() {
  size_t i;

  static const struct {
    const char *name;
    napi_callback callback;
  } funcs[] = {
#define F(name) { #name, nurkel_ ## name }
    F(tree_init),
    F(tree_open),
    F(tree_close),
    F(tree_root_hash_sync),
    F(tree_root_hash),
    F(tree_inject_sync),
    F(tree_inject),
    F(tree_get_sync),
    F(tree_get),
    F(tree_has_sync),
    F(tree_has),
    F(tree_insert_sync),
    F(tree_insert),
    F(tree_remove_sync),
    F(tree_remove),
    F(tree_prove_sync),
    F(tree_prove),
    F(tree_debug_info_sync),
    F(verify_sync),
    F(verify),
    F(destroy_sync),
    F(destroy),
    F(hash_sync),
    F(hash),
    F(compact_sync),
    F(compact),
    F(stat_sync),
    F(stat),

    /* TX Methods */
    F(tx_init),
    F(tx_open),
    F(tx_close),
    F(tx_root_hash_sync),
    F(tx_root_hash),
    F(tx_get_sync),
    F(tx_get),
    F(tx_has_sync),
    F(tx_has),
    F(tx_insert_sync),
    F(tx_insert),
    F(tx_remove_sync),
    F(tx_remove),
    F(tx_prove_sync),
    F(tx_prove),
    F(tx_commit_sync),
    F(tx_commit),
    F(tx_clear_sync),
    F(tx_clear),
    F(tx_inject_sync),
    F(tx_inject),
    F(tx_apply),
    F(tx_apply_sync),

    /* Iter methods */
    F(iter_init),
    F(iter_close),
    F(iter_next_sync),
    F(iter_next)
#undef F
  };

  for (i = 0; i < sizeof(funcs) / sizeof(funcs[0]); i++) {
    const char *name = funcs[i].name;
    napi_callback callback = funcs[i].callback;
    napi_value fn;

    NAPI_OK(napi_create_function(env,
                               name,
                               NAPI_AUTO_LENGTH,
                               callback,
                               NULL,
                               &fn));

    NAPI_OK(napi_set_named_property(env, exports, name, fn));
  }

  return 0;
}
