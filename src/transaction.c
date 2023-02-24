/**
 * transaction.c - transaction API.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#include <string.h>
#include <stdlib.h>
#include "transaction.h"

/*
 * Worker structs for async jobs.
 */

typedef struct nurkel_tx_open_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
} nurkel_tx_open_worker_t;

typedef struct nurkel_tx_close_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
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

typedef struct nurkel_tx_op_s {
  uint32_t op;
  size_t value_len;
  napi_ref key_ref;
  napi_ref value_ref;
  uint8_t *key;
  uint8_t *value;
} nurkel_tx_op_t;

typedef struct nurkel_tx_apply_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  nurkel_tx_op_t *in_ops;
  uint32_t in_ops_len;
} nurkel_tx_apply_worker_t;

/*
 * Nurkel related methods.
 */

static void
nurkel_ntx_init(nurkel_tx_t *ntx, nurkel_dlist_t *list) {
  ntx->tx = NULL;
  ntx->ntree = NULL;
  ntx->entry = NULL;
  ntx->ref = NULL;

  ntx->close_worker = NULL;
  ntx->workers = 0;
  ntx->state = nurkel_state_closed;
  ntx->must_cleanup = false;
  ntx->must_close_iters = false;

  ntx->iter_list = list;
  memset(ntx->init_root, 0, URKEL_HASH_SIZE);
}

static NURKEL_READY(ntx, nurkel_tx_t)

static napi_status
nurkel_tx_free(napi_env env, nurkel_tx_t *ntx) {
  CHECK(ntx->state == nurkel_state_closed);
  ntx->must_cleanup = false;

  nurkel_tree_t *ntree = ntx->ntree;

  NAPI_OK(napi_delete_reference(env, ntx->ref));
  nurkel_dlist_free(ntx->iter_list);
  free(ntx);

  NAPI_OK(napi_reference_unref(env, ntree->ref, NULL));
  return napi_ok;
}

/*
 * This acts as a queue for close and the destroy.
 */

static void
nurkel_tx_close_iters(napi_env env, nurkel_tx_t *ntx);

napi_status
nurkel_tx_final_check(napi_env env, nurkel_tx_t *ntx) {
  if (ntx->workers > 0)
    return napi_ok;

  if (ntx->must_close_iters) {
    nurkel_tx_close_iters(env, ntx);
    return napi_ok;
  }

  if (nurkel_dlist_len(ntx->iter_list) > 0)
    return napi_ok;

  if (ntx->close_worker != NULL) {
    CHECK(ntx->state == nurkel_state_open);
    ntx->state = nurkel_state_closing;
    nurkel_tx_close_worker_t *worker = ntx->close_worker;
    ntx->workers++;
    NAPI_OK(napi_queue_async_work(env, worker->work));
    return napi_ok;
  }

  if (ntx->must_cleanup)
    return nurkel_tx_free(env, ntx);

  return napi_ok;
}

static void
nurkel_tx_register_iter(nurkel_iter_t *niter) {
  nurkel_tx_t *ntx = niter->ntx;
  CHECK(niter->entry == NULL);
  nurkel_dlist_entry_t *entry = nurkel_dlist_insert(ntx->iter_list, niter);
  CHECK(entry != NULL);
  niter->entry = entry;
}

static void
nurkel_tx_unregister_iter(nurkel_iter_t *niter) {
  nurkel_tx_t *ntx = niter->ntx;
  nurkel_dlist_entry_t *entry = niter->entry;
  CHECK(entry != NULL);
  nurkel_dlist_remove(ntx->iter_list, entry);
}

static napi_status
nurkel_iter_final_check(napi_env env, nurkel_iter_t *niter);

static napi_status
nurkel_iter_queue_close_worker(napi_env env,
                               nurkel_iter_t *niter,
                               napi_deferred deferred);

static void
nurkel_tx_close_iters(napi_env env, nurkel_tx_t *ntx) {
  nurkel_dlist_entry_t *head = nurkel_dlist_iter(ntx->iter_list);

  while (head != NULL) {
    nurkel_iter_t *niter = nurkel_dlist_get_value(head);
    NAPI_OK(nurkel_iter_queue_close_worker(env, niter, NULL));
    NAPI_OK(nurkel_iter_final_check(env, niter));
    head = nurkel_dlist_iter_next(head);
  }

  ntx->must_close_iters = false;
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

napi_status
nurkel_tx_queue_close_worker(napi_env env,
                             nurkel_tx_t *ntx,
                             napi_deferred deferred);

static void
nurkel_ntx_destroy(napi_env env, void *data, void *hint) {
  (void)hint;

  CHECK(data != NULL);

  nurkel_tx_t *ntx = data;

  if (ntx->state != nurkel_state_closed && ntx->close_worker == NULL)
    NAPI_OK(nurkel_tx_queue_close_worker(env, ntx, NULL));

  ntx->must_cleanup = true;
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

static void
nurkel_tx_close_worker_exec(napi_env env, void *data) {
  (void)env;
  nurkel_tx_close_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  urkel_tx_destroy(ntx->tx);
  ntx->tx = NULL;
  worker->success = true;
}

static void
nurkel_tx_close_worker_complete(napi_env env, napi_status status, void *data) {
  nurkel_tx_close_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  nurkel_tree_t *ntree = ntx->ntree;

  /* Unset the queued close worker. */
  ntx->close_worker = NULL;
  ntx->state = nurkel_state_closed;
  ntx->workers--;

  if (worker->deferred != NULL) {
    napi_value result;

    if (status != napi_ok || worker->success != true) {
      NAPI_OK(nurkel_create_error(env,
                                  worker->err_res,
                                  "Failed to close transaction.",
                                  &result));
      NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    } else {
      NAPI_OK(napi_get_undefined(env, &result));
      NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
    }
  }

  nurkel_unregister_tx(ntx);
  NAPI_OK(napi_delete_async_work(env, worker->work));
  NAPI_OK(nurkel_tx_final_check(env, ntx));
  NAPI_OK(nurkel_final_check(env, ntree));
  free(worker);
}

napi_status
nurkel_tx_queue_close_worker(napi_env env,
                             nurkel_tx_t *ntx,
                             napi_deferred deferred) {
  CHECK(ntx != NULL);

  /* If close worker is not queued by the tx_close, then
   * we don't expect state to be open (it could be opening) */
  if (deferred != NULL) {
    CHECK(ntx->close_worker == NULL);
    CHECK(ntx->state == nurkel_state_open);
  }

  /* If we have already qeueud close worker, just ignore another request */
  if (ntx->close_worker != NULL)
    return napi_ok;

  napi_value workname;
  napi_status status;
  nurkel_tx_close_worker_t *worker;

  status = napi_create_string_latin1(env,
                                     "nurkel_tx_close",
                                     NAPI_AUTO_LENGTH,
                                     &workname);

  if (status != napi_ok)
    return status;

  worker = malloc(sizeof(nurkel_tx_close_worker_t));

  if (worker == NULL)
    return napi_generic_failure;

  WORKER_INIT(worker);
  worker->ctx = ntx;
  worker->deferred = deferred;

  status = napi_create_async_work(env,
                                  NULL,
                                  workname,
                                  nurkel_tx_close_worker_exec,
                                  nurkel_tx_close_worker_complete,
                                  worker,
                                  &worker->work);

  if (status != napi_ok) {
    free(worker);
    return status;
  }

  ntx->close_worker = worker;
  ntx->must_close_iters = true;

  return napi_ok;
}

NURKEL_METHOD(tx_init) {
  napi_value result;
  napi_status status;
  nurkel_tx_t *ntx = NULL;
  nurkel_dlist_t *iter_list = NULL;
  char *err;

  NURKEL_ARGV(1);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  iter_list = nurkel_dlist_alloc();
  JS_ASSERT(iter_list != NULL, JS_ERR_ALLOC);

  ntx = malloc(sizeof(nurkel_tx_t));
  JS_ASSERT_GOTO_THROW(ntx != NULL, JS_ERR_ALLOC);
  nurkel_ntx_init(ntx, iter_list);
  ntx->ntree = ntree;

  status = napi_create_external(env, ntx, nurkel_ntx_destroy, NULL, &result);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  /* Make sure we don't clean up transaction if we have iterators. */
  status = napi_create_reference(env, result, 0, &ntx->ref);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  /* We want the tree to live at least as long as the transaction. */
  status = napi_reference_ref(env, ntree->ref, NULL);
  if (status != napi_ok) {
    napi_delete_reference(env, ntx->ref);
    err = JS_ERR_NODE;
    goto throw;
  }

  return result;
throw:
  if (iter_list != NULL)
    nurkel_dlist_free(iter_list);

  if (ntx != NULL)
    free(ntx);

  JS_THROW(err);
}

NURKEL_EXEC(tx_open) {
  (void)env;

  nurkel_tx_open_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  nurkel_tree_t *ntree = ntx->ntree;
  urkel_tx_t *tx = NULL;

  tx = urkel_tx_create(ntree->tree, ntx->init_root);

  if (tx == NULL) {
    worker->success = false;
    worker->err_res = urkel_errno;
    return;
  }

  worker->success = true;
  ntx->tx = tx;
}

NURKEL_COMPLETE(tx_open) {
  (void)status;

  nurkel_tx_open_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  napi_value result;

  ntx->workers--;
  ntx->state = nurkel_state_closed;

  if (status != napi_ok || worker->success == false) {
    nurkel_unregister_tx(ntx);
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx open.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    ntx->state = nurkel_state_open;
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

/*
 * TODO: Reconsider if this needs to be an async method. Ofc, with this we
 * could also get rid of close and give that job sololy to GC (in tx_destroy).
 * Example would be urkel. It only checks root when necessary operation occurs.
 * We could use the similar approach and set root hash not on open but after.
 *  NOTE: Only tree->revert or HASH != NULL will end up reading disk for the
 * historical root.
 *  p.s. If we also add root cache similar to urkel, we could even not get
 * penalized for this operation??
 */

NURKEL_METHOD(tx_open) {
  napi_value result;
  napi_status status;
  napi_valuetype type;
  bool is_buffer = false;
  unsigned char *buffer = NULL;
  size_t buffer_len;
  nurkel_tx_open_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();

  JS_ASSERT(ntx->state != nurkel_state_open, "Transaction is already open.");
  JS_ASSERT(ntx->state != nurkel_state_opening, "Transaction is already opening.");
  JS_ASSERT(ntx->close_worker == NULL, "Transaction is closing.");
  JS_ASSERT(ntx->state != nurkel_state_closing, "Transaction is closing.");
  JS_ASSERT(ntx->state == nurkel_state_closed, "Transaction is not closed.");

  ntree = ntx->ntree;
  NURKEL_TREE_READY();

  status = napi_typeof(env, argv[1], &type);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);

  if (type != napi_null && type != napi_undefined) {
    JS_ASSERT(napi_is_buffer(env, argv[1], &is_buffer) == napi_ok, JS_ERR_NODE);
    JS_ASSERT(is_buffer, JS_ERR_ARG);

    status = napi_get_buffer_info(env, argv[1], (void **)&buffer, &buffer_len);
    JS_ASSERT(status == napi_ok, JS_ERR_ARG);
    JS_ASSERT(buffer_len == URKEL_HASH_SIZE, JS_ERR_ARG);
  }

  if (is_buffer) {
    memcpy(ntx->init_root, buffer, URKEL_HASH_SIZE);
  } else {
    urkel_root(ntree->tree, ntx->init_root);
  }

  worker = malloc(sizeof(nurkel_tx_open_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  NURKEL_CREATE_ASYNC_WORK(tx_open, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  /* Make sure Tree does not close and free while we are working with it. */
  nurkel_register_tx(ntx);
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    nurkel_unregister_tx(ntx);
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntx->workers++;
  ntx->state = nurkel_state_opening;
  return result;
}

NURKEL_METHOD(tx_close) {
  napi_value result;
  napi_status status;
  napi_deferred deferred;

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  status = napi_create_promise(env, &deferred, &result);
  JS_ASSERT(status == napi_ok, "Failed to create the promise.");

  status = nurkel_tx_queue_close_worker(env, ntx, deferred);
  JS_ASSERT(status == napi_ok, "Failed to setup the close worker.");
  JS_ASSERT(nurkel_tx_final_check(env, ntx) == napi_ok, "Failed to run final checks.");

  return result;
}

NURKEL_METHOD(tx_root_hash_sync) {
  napi_value result;
  uint8_t hash[URKEL_HASH_SIZE];

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  urkel_tx_root(ntx->tx, hash);

  JS_ASSERT(napi_create_buffer_copy(env,
                                    URKEL_HASH_SIZE,
                                    hash,
                                    NULL,
                                    &result) == napi_ok, JS_ERR_NODE);

  return result;
}

NURKEL_EXEC(tx_root_hash) {
  (void)env;
  nurkel_tx_t *ntx = NULL;
  nurkel_tx_root_hash_worker_t *worker = data;
  CHECK(worker != NULL);
  ntx = worker->ctx;

  urkel_tx_root(ntx->tx, worker->out_hash);
  worker->success = true;
}

NURKEL_COMPLETE(tx_root_hash) {
  napi_value result;
  nurkel_tx_root_hash_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to get tx_root_hash.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_create_buffer_copy(env,
                                    URKEL_HASH_SIZE,
                                    worker->out_hash,
                                    NULL,
                                    &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_root_hash) {
  napi_value result;
  napi_status status;
  nurkel_tx_root_hash_worker_t *worker;
  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  worker = malloc(sizeof(nurkel_tx_root_hash_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  NURKEL_CREATE_ASYNC_WORK(tx_root_hash, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntx->workers++;

  return result;
}

NURKEL_METHOD(tx_get_sync) {
  napi_value result;
  bool is_buffer;
  uint8_t *key_hash = NULL;
  size_t key_hash_len = 0;
  uint8_t value[URKEL_VALUE_SIZE];
  size_t value_len = 0;
  int res;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  JS_NAPI_OK_MSG(napi_is_buffer(env, argv[1], &is_buffer), JS_ERR_ARG);
  JS_ASSERT(is_buffer, JS_ERR_ARG);
  JS_NAPI_OK_MSG(napi_get_buffer_info(env,
                                      argv[1],
                                      (void **)&key_hash,
                                      &key_hash_len), JS_ERR_ARG);

  JS_ASSERT(key_hash_len == URKEL_HASH_SIZE, JS_ERR_ARG);

  res = urkel_tx_get(ntx->tx, value, &value_len, key_hash);

  if (res) {
    JS_NAPI_OK(napi_create_buffer_copy(env,
                                       value_len,
                                       value,
                                       NULL,
                                       &result));

    return result;
  }

  if (urkel_errno == URKEL_ENOTFOUND) {
    JS_NAPI_OK(napi_get_null(env, &result));
    return result;
  }

  JS_THROW_CODE(urkel_errno, "Failed to tx get key.");
}

NURKEL_EXEC(tx_get) {
  (void)env;

  nurkel_tx_get_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  int res = urkel_tx_get(ntx->tx,
                          worker->out_value,
                          &worker->out_value_len,
                          worker->in_key);

  if (res) {
    worker->success = true;
    worker->out_has_key = true;
    return;
  }

  if (urkel_errno == URKEL_ENOTFOUND) {
    worker->success = true;
    worker->out_has_key = false;
    return;
  }

  worker->err_res = urkel_errno;
  worker->success = false;
}

NURKEL_COMPLETE(tx_get) {
  napi_value result;
  nurkel_tx_get_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx get.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else if (!worker->out_has_key) {
    NAPI_OK(napi_get_null(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_create_buffer_copy(env,
                                    worker->out_value_len,
                                    worker->out_value,
                                    NULL,
                                    &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_get) {
  napi_value result;
  napi_status status;
  nurkel_tx_get_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  worker = malloc(sizeof(nurkel_tx_get_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  NURKEL_JS_HASH(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tx_get, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntx->workers++;
  return result;
}

NURKEL_METHOD(tx_has_sync) {
  napi_value result;
  napi_status status;
  uint8_t key_buffer[URKEL_HASH_SIZE];
  bool has_key = true;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();
  NURKEL_JS_HASH_OK(argv[1], key_buffer);

  if (!urkel_tx_has(ntx->tx, key_buffer)) {
    if (urkel_errno != URKEL_ENOTFOUND)
      JS_THROW(urkel_errors[urkel_errno]);

    has_key = false;
  }

  JS_NAPI_OK(napi_get_boolean(env, has_key, &result));
  return result;
}

NURKEL_EXEC(tx_has) {
  (void)env;

  nurkel_tx_has_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  if (!urkel_tx_has(ntx->tx, worker->in_key)) {
    if (urkel_errno != URKEL_ENOTFOUND) {
      worker->err_res = urkel_errno;
      worker->success = false;
      return;
    }

    worker->out_has_key = false;
    worker->success = true;
    return;
  }

  worker->out_has_key = true;
  worker->success = true;
}

NURKEL_COMPLETE(tx_has) {
  napi_value result;
  nurkel_tx_has_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx has.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_boolean(env, worker->out_has_key, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_has) {
  napi_value result;
  napi_status status;
  nurkel_tx_has_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  worker = malloc(sizeof(nurkel_tx_has_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  NURKEL_JS_HASH(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tx_has, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntx->workers++;

  return result;
}

NURKEL_METHOD(tx_insert_sync) {
  napi_value result;
  napi_status status;
  bool value_is_buffer;
  uint8_t *val_buffer = NULL;
  uint8_t key_buffer[URKEL_HASH_SIZE];
  uint8_t value_buffer[URKEL_VALUE_SIZE];
  size_t value_len = 0;

  NURKEL_ARGV(3);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();
  NURKEL_JS_HASH_OK(argv[1], key_buffer);

  JS_NAPI_OK(napi_get_undefined(env, &result));
  JS_NAPI_OK_MSG(napi_is_buffer(env, argv[2], &value_is_buffer), JS_ERR_ARG);
  JS_ASSERT(value_is_buffer == true, JS_ERR_ARG);
  JS_NAPI_OK_MSG(napi_get_buffer_info(env,
                                      argv[2],
                                      (void **)&val_buffer,
                                      &value_len), JS_ERR_ARG);
  JS_ASSERT(value_len <= URKEL_VALUE_SIZE, JS_ERR_ARG);
  memcpy(value_buffer, val_buffer, value_len);

  if (!urkel_tx_insert(ntx->tx, key_buffer, value_buffer, value_len))
    JS_THROW(urkel_errors[urkel_errno]);

  return result;
}

NURKEL_EXEC(tx_insert) {
  (void)env;

  nurkel_tx_insert_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  if (!urkel_tx_insert(ntx->tx, worker->in_key, worker->in_value,
                       worker->in_value_len)) {
    worker->success = false;
    worker->err_res = urkel_errno;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(tx_insert) {
  napi_value result;
  nurkel_tx_insert_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx insert.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_insert) {
  napi_value result;
  napi_status status;
  nurkel_tx_insert_worker_t *worker = NULL;

  NURKEL_ARGV(3);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  worker = malloc(sizeof(nurkel_tx_insert_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  NURKEL_JS_HASH(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  status = nurkel_get_buffer_copy(env,
                                  argv[2],
                                  worker->in_value,
                                  &worker->in_value_len,
                                  URKEL_VALUE_SIZE,
                                  true);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tx_insert, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntx->workers++;

  return result;
}

NURKEL_METHOD(tx_remove_sync) {
  napi_value result;
  napi_status status;
  uint8_t key_buffer[URKEL_HASH_SIZE];

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();
  NURKEL_JS_HASH_OK(argv[1], key_buffer);

  JS_NAPI_OK(napi_get_undefined(env, &result));

  if (!urkel_tx_remove(ntx->tx, key_buffer))
    JS_THROW(urkel_errors[urkel_errno]);

  return result;
}

NURKEL_EXEC(tx_remove) {
  (void)env;

  nurkel_tx_remove_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  if (!urkel_tx_remove(ntx->tx, worker->in_key)) {
    worker->err_res = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(tx_remove) {
  napi_value result;
  nurkel_tx_remove_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx remove.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }


  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_remove) {
  napi_value result;
  napi_status status;
  nurkel_tx_remove_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  worker = malloc(sizeof(nurkel_tx_remove_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  NURKEL_JS_HASH(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tx_remove, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntx->workers++;

  return result;
}

NURKEL_METHOD(tx_prove_sync) {
  napi_value result;
  napi_status status;
  uint8_t in_key[URKEL_HASH_SIZE];
  uint8_t *out_proof_raw = NULL;
  size_t out_proof_len;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();
  NURKEL_JS_HASH_OK(argv[1], in_key);

  if (!urkel_tx_prove(ntx->tx, &out_proof_raw, &out_proof_len, in_key))
    JS_THROW(urkel_errors[urkel_errno]);

  status = napi_create_external_buffer(env,
                                       out_proof_len,
                                       out_proof_raw,
                                       nurkel_buffer_finalize,
                                       NULL,
                                       &result);

  if (status != napi_ok)
    JS_THROW(JS_ERR_NODE);

  return result;
}

NURKEL_EXEC(tx_prove) {
  (void)env;

  nurkel_tx_prove_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  if (!urkel_tx_prove(ntx->tx, &worker->out_proof, &worker->out_proof_len,
                      worker->in_key)) {
    worker->err_res = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(tx_prove) {
  napi_value result;
  nurkel_tx_prove_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx prove.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    CHECK(worker->out_proof != NULL);
    NAPI_OK(napi_create_external_buffer(env,
                                        worker->out_proof_len,
                                        worker->out_proof,
                                        nurkel_buffer_finalize,
                                        NULL,
                                        &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_prove) {
  napi_value result;
  napi_status status;
  nurkel_tx_prove_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  worker = malloc(sizeof(nurkel_tx_prove_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;
  worker->out_proof = NULL;
  worker->out_proof_len = 0;

  NURKEL_JS_HASH(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tx_prove, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  ntx->workers++;

  return result;
}

NURKEL_METHOD(tx_commit_sync) {
  napi_value result;
  uint8_t tx_root[URKEL_HASH_SIZE];

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  if (!urkel_tx_commit(ntx->tx))
    JS_THROW(urkel_errors[urkel_errno]);

  urkel_tx_root(ntx->tx, tx_root);

  JS_NAPI_OK(napi_create_buffer_copy(env,
                                     URKEL_HASH_SIZE,
                                     tx_root,
                                     NULL,
                                     &result));
  return result;
}

NURKEL_EXEC(tx_commit) {
  (void)env;

  nurkel_tx_commit_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  if (!urkel_tx_commit(ntx->tx)) {
    worker->err_res = urkel_errno;
    worker->success = false;
    return;
  }

  urkel_tx_root(ntx->tx, worker->out_hash);
  worker->success = true;
}

NURKEL_COMPLETE(tx_commit) {
  napi_value result;
  nurkel_tx_commit_worker_t *worker = data;;
  nurkel_tx_t *ntx = worker->ctx;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx commit.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_create_buffer_copy(env,
                                    URKEL_HASH_SIZE,
                                    worker->out_hash,
                                    NULL,
                                    &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_commit) {
  napi_value result;
  napi_status status;
  nurkel_tx_commit_worker_t *worker;

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  worker = malloc(sizeof(nurkel_tx_commit_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  NURKEL_CREATE_ASYNC_WORK(tx_commit, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntx->workers++;

  return result;
}

NURKEL_METHOD(tx_clear_sync) {
  napi_value result;

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  urkel_tx_clear(ntx->tx);

  JS_NAPI_OK(napi_get_undefined(env, &result));

  return result;
}

NURKEL_EXEC(tx_clear) {
  (void)env;

  nurkel_tx_clear_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  urkel_tx_clear(ntx->tx);
  worker->success = true;
}

NURKEL_COMPLETE(tx_clear) {
  napi_value result;
  nurkel_tx_clear_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx clear.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }


  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_clear) {
  napi_value result;
  napi_status status;
  nurkel_tx_clear_worker_t *worker;

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  worker = malloc(sizeof(nurkel_tx_clear_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  NURKEL_CREATE_ASYNC_WORK(tx_clear, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntx->workers++;

  return result;
}

NURKEL_METHOD(tx_inject_sync) {
  napi_value result;
  napi_status status;
  uint8_t in_root[URKEL_HASH_SIZE];

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();
  NURKEL_JS_HASH_OK(argv[1], in_root);

  if (!urkel_tx_inject(ntx->tx, in_root))
    JS_THROW_CODE(urkel_errno, "Failed to tx_inject_sync.");

  JS_NAPI_OK(napi_get_undefined(env, &result));
  return result;
}

NURKEL_EXEC(tx_inject) {
  (void)env;

  nurkel_tx_inject_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  if (!urkel_tx_inject(ntx->tx, worker->in_root)) {
    worker->err_res = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(tx_inject) {
  napi_value result;
  nurkel_tx_inject_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  ntx->workers--;
  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx_inject.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_inject) {
  napi_value result;
  napi_status status;
  nurkel_tx_inject_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  worker = malloc(sizeof(nurkel_tx_inject_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  NURKEL_JS_HASH(argv[1], worker->in_root);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tx_inject, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntx->workers++;

  return result;
}

NURKEL_METHOD(tx_apply_sync) {
  napi_value result;
  uint32_t length, i;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  JS_NAPI_OK_MSG(napi_get_array_length(env, argv[1], &length), JS_ERR_ARG);
  JS_ASSERT(length != 0, JS_ERR_ARG);

  for (i = 0; i < length; i++) {
    napi_handle_scope scope;
    JS_NAPI_OK(napi_open_handle_scope(env, &scope));

    {
      napi_value element, js_op, js_key, js_value;
      uint32_t op;
      uint8_t *key, *value;
      size_t key_len, value_len;

      JS_NAPI_OK_MSG(napi_get_element(env, argv[1], i, &element), JS_ERR_ARG);

      JS_NAPI_OK_MSG(napi_get_element(env, element, 0, &js_op), JS_ERR_ARG);
      JS_NAPI_OK_MSG(napi_get_value_uint32(env, js_op, &op), JS_ERR_ARG);

      JS_NAPI_OK_MSG(napi_get_element(env, element, 1, &js_key), JS_ERR_ARG);
      JS_NAPI_OK(napi_get_buffer_info(env,
                                      js_key,
                                      (void **)&key,
                                      &key_len));
      JS_ASSERT(key_len == URKEL_HASH_SIZE, JS_ERR_ARG);

      switch (op) {
        case VTX_OP_INSERT: {
          JS_NAPI_OK_MSG(napi_get_element(env, element, 2, &js_value),
                         JS_ERR_ARG);
          JS_NAPI_OK(napi_get_buffer_info(env,
                                          js_value,
                                          (void **)&value,
                                          &value_len));
          if (!urkel_tx_insert(ntx->tx, key, value, value_len))
            JS_THROW(urkel_errors[urkel_errno]);
          break;
        }
        case VTX_OP_REMOVE: {
          if (!urkel_tx_remove(ntx->tx, key))
            JS_THROW(urkel_errors[urkel_errno]);
          break;
        }
        default: {
          JS_THROW(JS_ERR_INCORRECT_OP);
        }
      }
    }

    JS_NAPI_OK(napi_close_handle_scope(env, scope));
  }

  napi_get_undefined(env, &result);
  return result;
}

NURKEL_EXEC(tx_apply) {
  (void)env;

  uint32_t i;
  nurkel_tx_apply_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  for (i = 0; i < worker->in_ops_len; i++) {
    nurkel_tx_op_t *op = &worker->in_ops[i];

    switch (op->op) {
      case VTX_OP_INSERT: {
        if (!urkel_tx_insert(ntx->tx, op->key, op->value, op->value_len))
          goto fail;
        break;
      }
      case VTX_OP_REMOVE: {
        if (!urkel_tx_remove(ntx->tx, op->key))
          goto fail;
        break;
      }
      default: {
        /* We have already verified types. */
        CHECK(false);
      }
    }
  }
  worker->success = true;
  return;

fail:
  worker->err_res = urkel_errno;
  worker->success = false;
}

NURKEL_COMPLETE(tx_apply) {
  napi_value result;
  nurkel_tx_apply_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  uint32_t i;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx apply.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  for (i = 0; i < worker->in_ops_len; i++) {
    nurkel_tx_op_t *op = &worker->in_ops[i];

    if (op->key_ref != NULL)
      napi_delete_reference(env, op->key_ref);

    if (op->value_ref != NULL)
      napi_delete_reference(env, op->value_ref);
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker->in_ops);
  free(worker);
  NAPI_OK(nurkel_tx_final_check(env, ntx));
}

NURKEL_METHOD(tx_apply) {
  napi_value result;
  napi_status status;
  nurkel_tx_apply_worker_t *worker;
  uint32_t length, i, j;
  char *err;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  JS_NAPI_OK_MSG(napi_get_array_length(env, argv[1], &length), JS_ERR_ARG);
  JS_ASSERT(length != 0, JS_ERR_ARG);

  worker = malloc(sizeof(nurkel_tx_apply_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;
  worker->in_ops = NULL;
  worker->in_ops_len = 0;

  worker->in_ops = malloc(sizeof(nurkel_tx_op_t) * length);
  if (worker->in_ops == NULL) {
    free(worker);
    JS_THROW(JS_ERR_ALLOC);
  }
  memset(worker->in_ops, 0, sizeof(nurkel_tx_op_t) * length);
  worker->in_ops_len = length;

#define LOOP_THROW do { \
  fail = true;          \
  goto loop_end;        \
} while(0)

#define LOOP_NAPI_OK(status) do { \
  if (status != napi_ok) {        \
    fail = true;                  \
    goto loop_end;                \
  }                               \
} while(0)

  for (i = 0; i < length; i++) {
    napi_handle_scope scope;
    napi_open_handle_scope(env, &scope);
    bool fail = false;

    {
      napi_value element, js_op, js_key, js_value;
      nurkel_tx_op_t *op = &worker->in_ops[i];
      size_t key_len;

      LOOP_NAPI_OK(napi_get_element(env, argv[1], i, &element));
      LOOP_NAPI_OK(napi_get_element(env, element, 0, &js_op));
      LOOP_NAPI_OK(napi_get_element(env, element, 1, &js_key));
      LOOP_NAPI_OK(napi_get_value_uint32(env, js_op, &op->op));

      status = napi_get_buffer_info(env, js_key, (void **)&op->key, &key_len);

      if (status != napi_ok || key_len != URKEL_HASH_SIZE)
        LOOP_THROW;

      LOOP_NAPI_OK(napi_create_reference(env, js_key, 1, &op->key_ref));

      switch (op->op) {
        case VTX_OP_INSERT: {
          LOOP_NAPI_OK(napi_get_element(env, element, 2, &js_value));
          LOOP_NAPI_OK(napi_get_buffer_info(env,
                                            js_value,
                                            (void **)&op->value,
                                            &op->value_len));
          LOOP_NAPI_OK(napi_create_reference(env, js_value, 1, &op->value_ref));
          break;
        }
        case VTX_OP_REMOVE: {
          break;
        }
        default: {
          LOOP_THROW;
        }
      }
    }

loop_end:
    napi_close_handle_scope(env, scope);
    if (fail) {
      err = JS_ERR_ARG;
      goto throw;
    }
  }
#undef LOOP_NAPI_OK
#undef LOOP_THROW

  NURKEL_CREATE_ASYNC_WORK(tx_apply, worker, result);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    err = JS_ERR_NODE;
    goto throw;
  }

  ntx->workers++;

  return result;

throw:
  /* Clean up references */
  for (j = 0; j < i + 1; j++) {
    nurkel_tx_op_t *op = &worker->in_ops[i];

    if (op->key_ref != NULL)
      napi_delete_reference(env, op->key_ref);

    if (op->value_ref != NULL)
      napi_delete_reference(env, op->value_ref);
  }

  free(worker->in_ops);
  free(worker);
  JS_THROW(err);
}

napi_status
nurkel_iter_debug_info(napi_env env,
                       nurkel_iter_t *iter,
                       napi_value object);

napi_status
nurkel_tx_debug_info(napi_env env,
                     nurkel_tx_t *ntx,
                     napi_value object,
                     bool expand) {
  napi_status status;

  napi_value workers;
  napi_value state;
  napi_value iters;
  napi_value queued_close;
  napi_value queued_close_iters;
  napi_value iterators;

  uint32_t index = 0;
  nurkel_dlist_entry_t *head;

  RET_NAPI_NOK(napi_create_int32(env, ntx->workers, &workers));
  RET_NAPI_NOK(napi_create_int32(env, nurkel_dlist_len(ntx->iter_list), &iters));
  RET_NAPI_NOK(napi_create_uint32(env, ntx->state, &state));
  RET_NAPI_NOK(napi_get_boolean(env, ntx->close_worker != NULL, &queued_close));
  RET_NAPI_NOK(napi_get_boolean(env, ntx->must_close_iters, &queued_close_iters));

  RET_NAPI_NOK(napi_set_named_property(env, object, "workers", workers));
  RET_NAPI_NOK(napi_set_named_property(env, object, "state", state));
  RET_NAPI_NOK(napi_set_named_property(env, object, "iters", iters));
  RET_NAPI_NOK(
    napi_set_named_property(env, object, "isCloseQueued", queued_close));

  RET_NAPI_NOK(napi_set_named_property(env,
                                       object,
                                       "isIterCloseQueued",
                                       queued_close_iters));

  if (!expand)
    return napi_ok;

  RET_NAPI_NOK(
    napi_create_array_with_length(
      env,
      nurkel_dlist_len(ntx->iter_list),
      &iterators
    )
  );

  RET_NAPI_NOK(napi_set_named_property(env, object, "iterators", iterators));

  head = nurkel_dlist_iter(ntx->iter_list);

  while (head != NULL) {
    napi_value info;
    nurkel_iter_t *iter = nurkel_dlist_get_value(head);
    RET_NAPI_NOK(napi_create_object(env, &info));
    RET_NAPI_NOK(nurkel_iter_debug_info(env, iter, info));
    RET_NAPI_NOK(napi_set_element(env, iterators, index, info));
    index++;
    head = nurkel_dlist_iter_next(head);
  }

  return napi_ok;
}

/*
 * Iterators for the transaction.
 */

/* Workers */
typedef struct nurkel_iter_close_worker_s {
  WORKER_BASE_PROPS(nurkel_iter_t)
} nurkel_iter_close_worker_t;

typedef struct nurkel_iter_next_worker_s {
  WORKER_BASE_PROPS(nurkel_iter_t)
} nurkel_iter_next_worker_t;

static void
nurkel_niter_init(nurkel_iter_t *niter) {
  niter->iter = NULL;
  niter->ntx = NULL;

  niter->cache_max_size = 1;
  niter->cache_size = 0;
  niter->buffer = NULL;

  niter->entry = NULL;

  niter->state = nurkel_state_closed;
  niter->close_worker = NULL;
  niter->nexting = false;
  niter->must_cleanup = false;
}

static NURKEL_READY(niter, nurkel_iter_t)

static napi_status
nurkel_niter_free(napi_env env, nurkel_iter_t *niter) {
  nurkel_tx_t *ntx = niter->ntx;

  CHECK(niter->state == nurkel_state_closed);
  niter->must_cleanup = false;

  free(niter->buffer);
  free(niter);

  NAPI_OK(napi_reference_unref(env, ntx->ref, NULL));
  return napi_ok;
}

static napi_status
nurkel_iter_final_check(napi_env env, nurkel_iter_t *niter) {
  if (niter->nexting)
    return napi_ok;

  if (niter->close_worker != NULL) {
    CHECK(niter->state == nurkel_state_open);
    niter->state = nurkel_state_closing;
    nurkel_iter_close_worker_t *worker = niter->close_worker;
    niter->nexting = true;
    NAPI_OK(napi_queue_async_work(env, worker->work));
    return napi_ok;
  }

  if (niter->must_cleanup) {
    return nurkel_niter_free(env, niter);
  }

  return napi_ok;
}

static void
nurkel_niter_destroy(napi_env env, void *data, void *hint) {
  (void)hint;

  CHECK(data != NULL);

  nurkel_iter_t *niter = data;

  if (niter->state != nurkel_state_closed)
    NAPI_OK(nurkel_iter_queue_close_worker(env, niter, NULL));

  niter->must_cleanup = true;
  NAPI_OK(nurkel_iter_final_check(env, niter));
}

static void
nurkel_iter_close_worker_exec(napi_env env, void *data) {
  (void)env;
  nurkel_iter_close_worker_t *worker = data;
  nurkel_iter_t *niter = worker->ctx;

  urkel_iter_destroy(niter->iter);
  niter->iter = NULL;
  worker->success = true;
}

static void
nurkel_iter_close_worker_complete(napi_env env,
                                   napi_status status,
                                   void *data) {
  nurkel_iter_close_worker_t *worker = data;
  nurkel_iter_t *niter = worker->ctx;
  nurkel_tx_t *ntx = niter->ntx;

  niter->close_worker = NULL;
  niter->state = nurkel_state_closed;
  niter->nexting = false;

  if (worker->deferred != NULL) {
    napi_value result;

    if (status != napi_ok || worker->success != true) {
      NAPI_OK(nurkel_create_error(env,
                                  worker->err_res,
                                  "Failed to close iterator.",
                                  &result));
      NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    } else {
      NAPI_OK(napi_get_undefined(env, &result));
      NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
    }
  }

  nurkel_tx_unregister_iter(niter);
  NAPI_OK(napi_delete_async_work(env, worker->work));
  NAPI_OK(nurkel_iter_final_check(env, niter));
  NAPI_OK(nurkel_tx_final_check(env, ntx));
  free(worker);
}

static napi_status
nurkel_iter_queue_close_worker(napi_env env,
                               nurkel_iter_t *niter,
                               napi_deferred deferred) {
  CHECK(niter != NULL);

  if (deferred != NULL) {
    CHECK(niter->close_worker == NULL);
    CHECK(niter->state == nurkel_state_open);
  }

  /* We have already queued, ignore another request. */
  if (niter->close_worker != NULL)
    return napi_ok;

  napi_value workname;
  napi_status status;
  nurkel_iter_close_worker_t *worker;

  status = napi_create_string_latin1(env,
                                     "nurkel_iter_close",
                                     NAPI_AUTO_LENGTH,
                                     &workname);

  if (status != napi_ok)
    return status;

  worker = malloc(sizeof(nurkel_iter_close_worker_t));

  if (worker == NULL)
    return napi_generic_failure;

  WORKER_INIT(worker);
  worker->ctx = niter;
  worker->deferred = deferred;

  status = napi_create_async_work(env,
                                  NULL,
                                  workname,
                                  nurkel_iter_close_worker_exec,
                                  nurkel_iter_close_worker_complete,
                                  worker,
                                  &worker->work);

  if (status != napi_ok) {
    free(worker);
    return status;
  }

  niter->close_worker = worker;

  return napi_ok;
}

/* TODO: Test if mallocing on next is better than just having a buffer.
 * Could potentially avoid buffer_copy to js env. Not high prio. */
NURKEL_METHOD(iter_init) {
  napi_value result;
  napi_status status;
  nurkel_iter_t *niter;
  uint32_t cache_max_size;
  char *err;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  napi_get_value_uint32(env, argv[1], &cache_max_size);
  JS_ASSERT(cache_max_size > 0, JS_ERR_ARG);

  niter = malloc(sizeof(nurkel_iter_t));
  JS_ASSERT(niter != NULL, JS_ERR_ALLOC);
  nurkel_niter_init(niter);
  niter->state = nurkel_state_open;

  niter->ntx = ntx;
  niter->cache_max_size = cache_max_size;
  niter->buffer = malloc(sizeof(nurkel_iter_result_t) * cache_max_size);
  JS_ASSERT_GOTO_THROW(niter->buffer != NULL, JS_ERR_ALLOC);
  niter->iter = urkel_iter_create(ntx->tx);

  status = napi_create_external(env, niter, nurkel_niter_destroy, NULL, &result);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  /* Increment ref counter for the ntx, so it does not go out of scope */
  /* before iterator. */
  status = napi_reference_ref(env, ntx->ref, NULL);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  /* Add iterator as a dependency to the tx. */
  nurkel_tx_register_iter(niter);

  return result;

throw:
  if (niter->buffer != NULL)
    free(niter->buffer);

  if (niter != NULL)
    free(niter);

  JS_THROW(err);
}

NURKEL_METHOD(iter_close) {
  napi_value result;
  napi_status status;
  napi_deferred deferred;

  NURKEL_ARGV(1);
  NURKEL_ITER_CONTEXT();
  NURKEL_ITER_READY();

  status = napi_create_promise(env, &deferred, &result);
  JS_ASSERT(status == napi_ok, "Failed to create the promise.");

  status = nurkel_iter_queue_close_worker(env, niter, deferred);
  JS_ASSERT(status == napi_ok, "Failed to setup the close worker.");
  JS_ASSERT(nurkel_iter_final_check(env, niter) == napi_ok,
            "Failed to run final checks.");

  return result;
}

NURKEL_METHOD(iter_next_sync) {
  napi_value result;
  uint32_t *pi, i;
  int iter_s;

  NURKEL_ARGV(1);
  NURKEL_ITER_CONTEXT();
  NURKEL_ITER_READY();

  /* Async worker may be working on it already. */
  JS_ASSERT(!niter->nexting, "Already nexting.");

  pi = &niter->cache_size;
  for (*pi = 0; *pi < niter->cache_max_size; (*pi)++) {
    iter_s = urkel_iter_next(niter->iter,
                             (uint8_t *)&niter->buffer[*pi].key,
                             (uint8_t *)&niter->buffer[*pi].value,
                             &niter->buffer[*pi].size);

    if (!iter_s && urkel_errno == URKEL_EITEREND)
      break;

    if (!iter_s)
      JS_THROW_CODE(urkel_errno, "Failed to get next items in the iterator.");
  }

  napi_create_array_with_length(env, niter->cache_size, &result);

  for (i = 0; i < *pi; i++) {
    napi_handle_scope scope;
    JS_NAPI_OK(napi_open_handle_scope(env, &scope));

    nurkel_iter_result_t *item = (niter->buffer + i);
    napi_value object;
    napi_value js_key;
    napi_value js_value;

    JS_NAPI_OK(napi_create_object(env, &object));
    JS_NAPI_OK(napi_create_buffer_copy(env,
                                       URKEL_HASH_SIZE,
                                       item->key,
                                       NULL,
                                       &js_key));
    JS_NAPI_OK(napi_create_buffer_copy(env,
                                       item->size,
                                       item->value,
                                       NULL,
                                       &js_value));

    JS_NAPI_OK(napi_set_named_property(env, object, "key", js_key));
    JS_NAPI_OK(napi_set_named_property(env, object, "value", js_value));
    JS_NAPI_OK(napi_set_element(env, result, i, object));
    JS_NAPI_OK(napi_close_handle_scope(env, scope));
  }

  return result;
}

NURKEL_EXEC(iter_next) {
  (void)env;

  nurkel_iter_next_worker_t *worker = data;
  nurkel_iter_t *niter = worker->ctx;

  uint32_t *pi;
  int iter_s;

  pi = &niter->cache_size;

  for (*pi = 0; *pi < niter->cache_max_size; (*pi)++) {
    iter_s = urkel_iter_next(niter->iter,
                             (uint8_t *)&niter->buffer[*pi].key,
                             (uint8_t *)&niter->buffer[*pi].value,
                             &niter->buffer[*pi].size);

    if (!iter_s && urkel_errno == URKEL_EITEREND)
      break;

    if (!iter_s) {
      worker->success = false;
      worker->err_res = urkel_errno;
      return;
    }
  }

  worker->success = true;
}

NURKEL_COMPLETE(iter_next) {
  napi_value result;
  nurkel_iter_next_worker_t *worker = data;
  nurkel_iter_t *niter = worker->ctx;
  uint32_t i;

  niter->nexting = false;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to iter_next.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    NAPI_OK(napi_delete_async_work(env, worker->work));
    free(worker);
    NAPI_OK(nurkel_iter_final_check(env, niter));
    return;
  }

  napi_create_array_with_length(env, niter->cache_size, &result);

  for (i = 0; i < niter->cache_size; i++) {
    napi_handle_scope scope;
    NAPI_OK(napi_open_handle_scope(env, &scope));

    nurkel_iter_result_t *item = (niter->buffer + i);
    napi_value object;
    napi_value js_key;
    napi_value js_value;

    NAPI_OK(napi_create_object(env, &object));
    NAPI_OK(napi_create_buffer_copy(env,
                                       URKEL_HASH_SIZE,
                                       item->key,
                                       NULL,
                                       &js_key));
    NAPI_OK(napi_create_buffer_copy(env,
                                       item->size,
                                       item->value,
                                       NULL,
                                       &js_value));

    NAPI_OK(napi_set_named_property(env, object, "key", js_key));
    NAPI_OK(napi_set_named_property(env, object, "value", js_value));
    NAPI_OK(napi_set_element(env, result, i, object));
    NAPI_OK(napi_close_handle_scope(env, scope));
  }

  NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
  NAPI_OK(nurkel_iter_final_check(env, niter));
}

NURKEL_METHOD(iter_next) {
  napi_value result;
  napi_status status;
  nurkel_iter_next_worker_t *worker;

  NURKEL_ARGV(1);
  NURKEL_ITER_CONTEXT();
  NURKEL_ITER_READY();

  JS_ASSERT(!niter->nexting, "Already nexting.");
  worker = malloc(sizeof(nurkel_iter_next_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = niter;

  NURKEL_CREATE_ASYNC_WORK(iter_next, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  niter->nexting = true;
  return result;
}

napi_status
nurkel_iter_debug_info(napi_env env,
                       nurkel_iter_t *niter,
                       napi_value object) {
  napi_status status;

  napi_value nexting;
  napi_value state;
  napi_value cache_max_size;
  napi_value cache_size;
  napi_value buffer_size;
  napi_value queued_close;

  size_t alloc_size = niter->cache_max_size * sizeof(nurkel_iter_result_t);

  RET_NAPI_NOK(napi_get_boolean(env, niter->nexting, &nexting));
  RET_NAPI_NOK(napi_create_int32(env, niter->state, &state));
  RET_NAPI_NOK(napi_create_int32(env, niter->cache_max_size, &cache_max_size));
  RET_NAPI_NOK(napi_create_int32(env, niter->cache_size, &cache_size));
  RET_NAPI_NOK(napi_create_int64(env, alloc_size, &buffer_size));
  RET_NAPI_NOK(
    napi_get_boolean(env, niter->close_worker != NULL, &queued_close));

  RET_NAPI_NOK(napi_set_named_property(env, object, "nexting", nexting));
  RET_NAPI_NOK(napi_set_named_property(env, object, "state", state));
  RET_NAPI_NOK(
    napi_set_named_property(env, object, "cacheMaxSize", cache_max_size));
  RET_NAPI_NOK(napi_set_named_property(env, object, "cacheSize", cache_size));
  RET_NAPI_NOK(napi_set_named_property(env, object, "bufferSize", buffer_size));
  RET_NAPI_NOK(
    napi_set_named_property(env, object, "isCloseQueued", queued_close));

  return napi_ok;
}
