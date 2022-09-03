/**
 * transaction.c - transaction API.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#include <string.h>
#include <stdlib.h>
#include "transaction.h"

void
nurkel_ntx_init(nurkel_tx_t *ntx) {
  ntx->tx = NULL;
  ntx->ntree = NULL;
  ntx->entry = NULL;
  ntx->close_worker = NULL;
  ntx->workers = 0;
  ntx->is_open = false;
  ntx->is_opening = false;
  ntx->is_closing = false;
  ntx->should_close = false;
  ntx->should_cleanup = false;
  memset(ntx->init_root, 0, URKEL_HASH_SIZE);
}

enum inst_state
nurkel_tx_ready(nurkel_tx_t *ntx) {
  if (!ntx->is_open)
    return inst_state_is_closed;

  if (ntx->is_closing)
    return inst_state_is_closing;

  if (ntx->is_opening)
    return inst_state_is_opening;

  if (ntx->should_close)
    return inst_state_should_close;

  return inst_state_ok;
}

napi_status
nurkel_tx_close_try_close(napi_env env, nurkel_tx_t *ntx) {
  nurkel_tx_close_worker_t *close_worker;

  if (!ntx->should_close)
    return napi_ok;

  if (ntx->workers > 0)
    return napi_ok;

  CHECK(ntx->workers == 0);
  ntx->is_closing = true;
  close_worker = ntx->close_worker;
  return napi_queue_async_work(env, close_worker->work);
}

void
nurkel_tx_destroy(napi_env env, void *data, void *hint) {
  (void)hint;

  CHECK(data != NULL);

  nurkel_tx_t *ntx = data;
  nurkel_tree_t *ntree = ntx->ntree;

  if (ntree->is_closing) {
    ntx->should_cleanup = true;
    return;
  }

  if (ntx->is_open || ntx->is_opening) {
    nurkel_tx_close_params_t params = {
      .env = env,
      .ctx = ntx,
      .destroy = true,
      .promise = false,
      .promise_result = NULL
    };

    NAPI_OK(nurkel_tx_close_work(params));
    return;
  }

  if (ntx->should_close) {
    nurkel_tx_close_worker_t *worker = ntx->close_worker;
    worker->in_destroy = true;
    return;
  }

  nurkel_close_try_close(env, ntree);
  /* We only allow tree to go out of scope after all txs go out of scope. */
  NAPI_OK(napi_reference_unref(env, ntree->ref, NULL));

  free(ntx);
}

NURKEL_EXEC(tx_close);
NURKEL_COMPLETE(tx_close);

napi_status
nurkel_tx_close_work(nurkel_tx_close_params_t params) {
  napi_value workname;
  napi_status status;
  nurkel_tx_close_worker_t *worker;
  nurkel_tx_t *ntx = params.ctx;

  CHECK(ntx != NULL);

  /* TODO: Dive into segfault here.
   * Possible scenario?
   *   ntx is freed before even get here from the nurkel_close_work_txs?.
   */
  if (ntx->is_closing || ntx->should_close) {
    if (params.promise) {
      return napi_throw_error(params.env,
                              "",
                              "Failed to tx_close, it is already closing.");
    }

    return napi_ok;
  }

  status = napi_create_string_latin1(params.env,
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
  worker->in_destroy = params.destroy;

  if (params.promise) {
    status = napi_create_promise(params.env,
                                 &worker->deferred,
                                 params.promise_result);

    if (status != napi_ok) {
      free(worker);
      return status;
    }
  }

  status = napi_create_async_work(params.env,
                                  NULL,
                                  workname,
                                  NURKEL_EXEC_NAME(tx_close),
                                  NURKEL_COMPLETE_NAME(tx_close),
                                  worker,
                                  &worker->work);
  if (status != napi_ok) {
    free(worker);
    return status;
  }

  if (ntx->workers > 0 || ntx->is_opening) {
    ntx->should_close = true;
    ntx->close_worker = worker;
    return napi_ok;
  }

  CHECK(ntx->workers == 0);
  ntx->is_closing = true;
  status = napi_queue_async_work(params.env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(params.env, worker->work);
    free(worker);
    return napi_generic_failure;
  }

  return napi_ok;
}

NURKEL_METHOD(tx_init) {
  napi_value result;
  napi_status status;
  nurkel_tx_t *ntx;

  NURKEL_ARGV(1);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  ntx = malloc(sizeof(nurkel_tx_t));
  JS_ASSERT(ntx != NULL, JS_ERR_ALLOC);
  nurkel_ntx_init(ntx);
  ntx->ntree = ntree;

  /* We want the tree to live at least as long as the transaction. */
  status = napi_reference_ref(env, ntree->ref, NULL);
  if (status != napi_ok) {
    free(ntx);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_create_external(env, ntx, nurkel_tx_destroy, NULL, &result);
  if (status != napi_ok) {
    napi_reference_unref(env, ntree->ref, NULL);
    free(ntx);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
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
  nurkel_tree_t *ntree = ntx->ntree;
  napi_value result;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    ntx->is_opening = false;

    nurkel_unregister_tx(ntx);
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx open.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    ntx->is_open = true;
    ntx->is_opening = false;
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  /* At this point, tree could have started closing and waiting for tx ? */
  /* We are closing right away.. Tree must have said so.. */
  if (ntx->should_close) {
    /* Sanity check */
    CHECK(ntree->should_close == true);
    nurkel_tx_close_try_close(env, ntx);
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
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
  nurkel_tree_t *ntree;
  nurkel_tx_t *ntx;
  nurkel_tx_open_worker_t *worker;

  NURKEL_ARGV(2);

  /* Grab the tx */
  status = napi_get_value_external(env, argv[0], (void **)&ntx);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(ntx != NULL, JS_ERR_ARG);

  JS_ASSERT(!ntx->is_open && !ntx->is_opening, "Transaction is already open.");
  JS_ASSERT(!ntx->is_closing, "Transaction is closing.");
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
  ntx->is_opening = true;
  ntx->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    nurkel_unregister_tx(ntx);
    ntree->is_opening = false;
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}

NURKEL_EXEC(tx_close) {
  (void)env;
  nurkel_tx_close_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  urkel_tx_destroy(ntx->tx);
  ntx->tx = NULL;
  worker->success = true;
}

NURKEL_COMPLETE(tx_close) {
  nurkel_tx_close_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  nurkel_tree_t *ntree = ntx->ntree;
  napi_value result;

  ntx->is_closing = false;
  ntx->is_opening = false;
  ntx->is_open = false;
  ntx->should_close = false;
  ntx->close_worker = NULL;

  if (worker->deferred != NULL && status != napi_ok) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to tx close.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else if (worker->deferred != NULL) {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }


  nurkel_unregister_tx(ntx);
  NAPI_OK(napi_delete_async_work(env, worker->work));
  NAPI_OK(nurkel_close_try_close(env, ntree));

  if (worker->in_destroy || ntx->should_cleanup) {
    NAPI_OK(napi_reference_unref(env, ntree->ref, NULL));
    free(ntx);
  }
  free(worker);
}

NURKEL_METHOD(tx_close) {
  napi_value result;
  napi_status status;
  nurkel_tx_close_params_t params;

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  params.env = env;
  params.ctx = ntx;
  params.promise = true;
  params.promise_result = &result;
  params.destroy = false;

  status = nurkel_tx_close_work(params);
  JS_ASSERT(status == napi_ok, "Failed to start tx close worker.");

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
    NAPI_OK(nurkel_tx_close_try_close(env, ntx));
  } else {
    NAPI_OK(napi_create_buffer_copy(env,
                                    URKEL_HASH_SIZE,
                                    worker->out_hash,
                                    NULL,
                                    &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  NAPI_OK(nurkel_tx_close_try_close(env, ntx));
  free(worker);
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

  ntx->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}

NURKEL_METHOD(tx_get_sync) {
  napi_value result;
  bool is_buffer;
  uint8_t *key_hash = NULL;
  size_t key_hash_len = 0;
  uint8_t value[URKEL_VALUE_SIZE];
  size_t value_len = 0;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  JS_NAPI_OK(napi_is_buffer(env, argv[1], &is_buffer), JS_ERR_ARG);
  JS_ASSERT(is_buffer, JS_ERR_ARG);
  JS_NAPI_OK(napi_get_buffer_info(env,
                                  argv[1],
                                  (void **)&key_hash,
                                  &key_hash_len), JS_ERR_ARG);

  JS_ASSERT(key_hash_len == URKEL_HASH_SIZE, JS_ERR_ARG);

  if (!urkel_tx_get(ntx->tx, value, &value_len, key_hash))
    JS_THROW_CODE(urkel_errors[urkel_errno - 1], "Failed to tx get key.");

  JS_NAPI_OK(napi_create_buffer_copy(env,
                                     value_len,
                                     value,
                                     NULL,
                                     &result), JS_ERR_NODE);

  return result;
}

NURKEL_EXEC(tx_get) {
  (void)env;

  nurkel_tx_get_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  if (!urkel_tx_get(ntx->tx,
                    worker->out_value,
                    &worker->out_value_len,
                    worker->in_key)) {
    worker->err_res = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
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
  } else {
    NAPI_OK(napi_create_buffer_copy(env,
                                    worker->out_value_len,
                                    worker->out_value,
                                    NULL,
                                    &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  NAPI_OK(nurkel_tx_close_try_close(env, ntx));
  free(worker);
  return;
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

  /* TODO: Does nodejs buffer live long enough for us to use underlying buffer
   * instead of copy? */
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

  ntx->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

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
      JS_THROW(urkel_errors[urkel_errno - 1]);

    has_key = false;
  }

  JS_NAPI_OK(napi_get_boolean(env, has_key, &result), JS_ERR_NODE);
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
  NAPI_OK(nurkel_tx_close_try_close(env, ntx));
  free(worker);
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

  ntx->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

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

  JS_NAPI_OK(napi_get_undefined(env, &result), JS_ERR_NODE);
  JS_NAPI_OK(napi_is_buffer(env, argv[2], &value_is_buffer), JS_ERR_ARG);
  JS_ASSERT(value_is_buffer == true, JS_ERR_ARG);
  JS_NAPI_OK(napi_get_buffer_info(env,
                                  argv[2],
                                  (void **)&val_buffer,
                                  &value_len), JS_ERR_ARG);
  JS_ASSERT(value_len <= URKEL_VALUE_SIZE, JS_ERR_ARG);
  memcpy(value_buffer, val_buffer, value_len);

  if (!urkel_tx_insert(ntx->tx, key_buffer, value_buffer, value_len))
    JS_THROW(urkel_errors[urkel_errno - 1]);

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
  NAPI_OK(nurkel_tx_close_try_close(env, ntx));
  free(worker);
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

  ntx->workers++;

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

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

  JS_NAPI_OK(napi_get_undefined(env, &result), JS_ERR_NODE);

  if (!urkel_tx_remove(ntx->tx, key_buffer))
    JS_THROW(urkel_errors[urkel_errno - 1]);

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
  NAPI_OK(nurkel_tx_close_try_close(env, ntx));
  free(worker);
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
    JS_THROW(JS_ERR_ARG);
  }

  ntx->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

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
    JS_THROW(urkel_errors[urkel_errno - 1]);

  /* TODO: Move instead of copy */
  status = napi_create_buffer_copy(env,
                                   out_proof_len,
                                   out_proof_raw,
                                   NULL,
                                   &result);

  free(out_proof_raw);

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
    /* TODO: Move instead of copy */
    NAPI_OK(napi_create_buffer_copy(env,
                                    worker->out_proof_len,
                                    worker->out_proof,
                                    NULL,
                                    &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  NAPI_OK(nurkel_tx_close_try_close(env, ntx));

  if (worker->out_proof != NULL)
    free(worker->out_proof);
  free(worker);
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

  ntx->workers++;
  status = napi_queue_async_work(env, worker->work);
  if (status != napi_ok) {
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  return result;
}

NURKEL_METHOD(tx_commit_sync) {
  napi_value result;
  uint8_t tx_root[URKEL_HASH_SIZE];

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  if (!urkel_tx_commit(ntx->tx))
    JS_THROW(urkel_errors[urkel_errno - 1]);

  urkel_tx_root(ntx->tx, tx_root);

  JS_NAPI_OK(napi_create_buffer_copy(env,
                                     URKEL_HASH_SIZE,
                                     tx_root,
                                     NULL,
                                     &result), JS_ERR_NODE);
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
  NAPI_OK(nurkel_tx_close_try_close(env, ntx));
  free(worker);
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

  ntx->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}

NURKEL_METHOD(tx_clear_sync) {
  napi_value result;

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  urkel_tx_clear(ntx->tx);

  JS_NAPI_OK(napi_get_undefined(env, &result), JS_ERR_NODE);

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
  NAPI_OK(nurkel_tx_close_try_close(env, ntx));
  free(worker);
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

  ntx->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

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
    JS_THROW_CODE(urkel_errors[urkel_errno - 1], "Failed to tx_inject_sync.");

  JS_NAPI_OK(napi_get_undefined(env, &result), JS_ERR_NODE);
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
  NAPI_OK(nurkel_tx_close_try_close(env, ntx));
  free(worker);
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

  ntx->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntx->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}
