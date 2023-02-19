/**
 * common.h - common things to nurkel.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#ifndef _NURKEL_COMMON_H
#define _NURKEL_COMMON_H

#include <stdbool.h>
#include <stdint.h>
#include <urkel.h>
#include <node_api.h>

/*
 * Urkel errors
 */

#define URKEL_UNKNOWN -1
#define URKEL_OK 0

/* Errnos start with 1, 0 = everything's ok. */
extern const char *urkel_errors[];
extern const int urkel_errors_len;

/*
 * Urkel constants
 */

#define URKEL_HASH_SIZE 32
#define URKEL_VALUE_SIZE 1023
#define URKEL_PROOF_SIZE 17957

enum nurkel_state {
  nurkel_state_closed = 0,
  nurkel_state_opening = 1,
  nurkel_state_open = 2,
  nurkel_state_closing = 3
};

enum nurkel_state_err {
  nurkel_state_err_ok = 0,
  nurkel_state_err_unknown = 1,
  nurkel_state_err_opening = 2,
  nurkel_state_err_closing = 3,
  nurkel_state_err_closed = 4,
};

extern const char *state_errors[];

/*
 * Nurkel errors
 */

#define ERR_UNKNOWN "ERR_UNKNOWN"
#define JS_ERR_INIT "Failed to initialize."
#define JS_ERR_NOT_IMPL "Not implemented."
#define JS_ERR_NOT_SUPPORTED "Not supported."
#define JS_ERR_ARG "Invalid argument."
#define JS_ERR_ALLOC "Allocation failed."
#define JS_ERR_NODE "Node internal error."
#define JS_ERR_UNKNOWN "Unknown internal error."
#define JS_ERR_URKEL_DESTROY "Urkel destroy failed."
#define JS_ERR_INCORRECT_OP "Bulk apply OPERATION is UNKNOWN."

/*
 * Main state management structs.
 */

typedef struct nurkel_tx_entry_s {
  struct nurkel_tx_s *ntx;

  struct nurkel_tx_entry_s *prev;
  struct nurkel_tx_entry_s *next;
} nurkel_tx_entry_t;

typedef struct nurkel_tree_s {
  urkel_t *tree;
  napi_ref ref;

  /** This is incremented every time async work is running and we can't close */
  uint32_t workers;

  /** Transactions that depend on the tree. */
  uint32_t tx_len;
  nurkel_tx_entry_t *tx_head;

  /** Current state of the tree. */
  enum nurkel_state state;

  /* If this is set, it means tree needs closing. */
  void *close_worker;

  bool must_close_txs;
  /* If this is set, it means tree needs freeing. */
  bool must_cleanup;
} nurkel_tree_t;

typedef struct nurkel_tx_s {
  nurkel_tree_t *ntree;
  urkel_tx_t *tx;

  /** Nurkel tree linked list entry. */
  nurkel_tx_entry_t *entry;
  /** On open, load the Root hash here. */
  uint8_t init_root[URKEL_HASH_SIZE];

  /** Current state of the transaction. */
  enum nurkel_state state;
  /** This is incremented every time async work is running and we can't close */
  uint32_t workers;
  /** If this is set, it means transaction needs closing. */
  void *close_worker;
  /** If this is set, it means transaction needs freeing. */
  bool must_cleanup;
} nurkel_tx_t;

/*
 * Close worker related.
 */

#define WORKER_CLOSE_PARAMS(ctx_t) \
  napi_env env;                    \
  ctx_t *ctx;                      \
  napi_value *promise_result;      \
  bool promise;                    \
  bool destroy;                    \

/*
 * Tree close worker.
 */

typedef struct nurkel_close_params_s {
  WORKER_CLOSE_PARAMS(nurkel_tree_t)
} nurkel_close_params_t;

/*
 * Transaction close worker.
 */

typedef struct nurkel_tx_close_params_s {
  WORKER_CLOSE_PARAMS(nurkel_tx_t)
} nurkel_tx_close_params_t;

#undef WORKER_CLOSE_PARAMS

#endif /* _NURKEL_COMMON_H */
