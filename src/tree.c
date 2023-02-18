/**
 * tree.c - Tree API bindings.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#include <stdlib.h>
#include <string.h>
#include <node_api.h>
#include "common.h"
#include "util.h"
#include "tree.h"

void
nurkel_ntree_init(nurkel_tree_t *ntree) {
  ntree->tree = NULL;
  ntree->ref = NULL;
  ntree->close_worker = NULL;
  ntree->workers = 0;
  ntree->is_open = false;
  ntree->is_opening = false;
  ntree->is_closing = false;
  ntree->should_close = false;
  ntree->should_cleanup = false;

  // Init list
  ntree->tx_len = 0;
  ntree->tx_head = NULL;
}

enum inst_state
nurkel_tree_ready(nurkel_tree_t *ntree) {
  if (!ntree->is_open)
    return inst_state_is_closed;

  if (ntree->is_closing)
    return inst_state_is_closing;

  if (ntree->is_opening)
    return inst_state_is_opening;

  if (ntree->should_close)
    return inst_state_should_close;

  return inst_state_ok;
}

napi_status
nurkel_close_try_close(napi_env env, nurkel_tree_t *ntree) {
  nurkel_close_worker_t *close_worker;

  if (!ntree->should_close)
    return napi_ok;

  if (ntree->tx_len > 0)
    return napi_ok;

  if (ntree->workers > 0)
    return napi_ok;

  ntree->is_closing = true;
  close_worker = ntree->close_worker;
  return napi_queue_async_work(env, close_worker->work);
}

void
nurkel_register_tx(struct nurkel_tx_s *ntx) {
  nurkel_tree_t *ntree = ntx->ntree;
  nurkel_tx_entry_t *entry = malloc(sizeof(nurkel_tx_entry_t));

  CHECK(entry != NULL);

  ntree->tx_len++;

  entry->next = NULL;
  entry->prev = NULL;
  entry->ntx = ntx;

  ntx->entry = entry;

  if (ntree->tx_head == NULL) {
    ntree->tx_head = entry;
    return;
  }

  CHECK(ntree->tx_head->prev == NULL);

  ntree->tx_head->prev = entry;
  entry->next = ntree->tx_head;
  ntree->tx_head = entry;
}

void
nurkel_unregister_tx(struct nurkel_tx_s *ntx) {
  nurkel_tree_t *ntree = ntx->ntree;
  nurkel_tx_entry_t *entry = ntx->entry;

  CHECK(entry != NULL);
  CHECK(ntree->tx_len > 0);

  ntree->tx_len--;
  ntx->entry = NULL;

  if (ntree->tx_len == 0)
    ntree->tx_head = NULL;

  if (entry->prev != NULL) {
    entry->prev->next = entry->next;
  } else {
    ntree->tx_head = entry->next;
  }

  if (entry->next != NULL)
    entry->next->prev = entry->prev;

  free(entry);
}

static void
nurkel_close_work_txs(nurkel_close_params_t params) {
  napi_env env = params.env;
  nurkel_tree_t *ntree = params.ctx;
  nurkel_tx_entry_t *head = ntree->tx_head;

  while (head != NULL) {
    nurkel_tx_queue_close_worker(env, head->ntx, NULL);
    nurkel_tx_final_check(env, head->ntx);
    head = head->next;
  }
}

NURKEL_EXEC(tree_close);
NURKEL_COMPLETE(tree_close);

static napi_status
nurkel_close_work(nurkel_close_params_t params) {
  napi_value workname;
  napi_status status;
  nurkel_close_worker_t *worker;
  nurkel_tree_t *ntree = params.ctx;

  if (ntree->is_closing || ntree->should_close) {
    if (params.promise) {
      return napi_throw_error(params.env,
                              "",
                              "Failed to close, it is already closing.");
    }
    return napi_ok;
  }

  status = napi_create_string_latin1(params.env,
                                     "nurkel_close",
                                     NAPI_AUTO_LENGTH,
                                     &workname);
  if (status != napi_ok)
    return status;

  worker = malloc(sizeof(nurkel_close_worker_t));
  if (worker == NULL)
    return napi_generic_failure;

  WORKER_INIT(worker);
  worker->ctx = ntree;
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
                                  NURKEL_EXEC_NAME(tree_close),
                                  NURKEL_COMPLETE_NAME(tree_close),
                                  worker,
                                  &worker->work);
  if (status != napi_ok) {
    free(worker);
    return status;
  }

  if (ntree->workers > 0 || ntree->tx_len > 0 || ntree->is_opening) {
    nurkel_close_work_txs(params);
    ntree->should_close = true;
    ntree->close_worker = worker;
    return napi_ok;
  }

  ntree->is_closing = true;
  status = napi_queue_async_work(params.env, worker->work);

  if (status != napi_ok) {
    ntree->is_closing = false;
    napi_delete_async_work(params.env, worker->work);
    free(worker);
    return napi_generic_failure;
  }

  return napi_ok;
}

void
nurkel_ntree_destroy(napi_env env, void *data, void *hint) {
  (void)hint;

  if (!data)
    return;

  nurkel_tree_t *ntree = data;

  if (ntree->is_closing) {
    ntree->should_cleanup = true;
    return;
  }

  if (ntree->is_open || ntree->is_opening) {
    /* User forgot to clean up before ref got out of the scope . */
    /* Make sure we do the clean up. */
    nurkel_close_params_t params = {
      .env = env,
      .ctx = ntree,
      .destroy = true,
      .promise = false,
      .promise_result = NULL
    };

    NAPI_OK(nurkel_close_work(params));
    return;
  }

  if (ntree->should_close) {
    nurkel_close_worker_t *worker = ntree->close_worker;
    worker->in_destroy = true;
    return;
  }

  napi_delete_reference(env, ntree->ref);
  free(ntree);
}

NURKEL_METHOD(tree_init) {
  (void)info;
  napi_status status;
  napi_value result;
  nurkel_tree_t *ntree;

  ntree = malloc(sizeof(nurkel_tree_t));
  CHECK(ntree != NULL);
  nurkel_ntree_init(ntree);

  status = napi_create_external(env,
                                ntree,
                                nurkel_ntree_destroy,
                                NULL,
                                &result);

  if (status != napi_ok) {
    free(ntree);
    JS_THROW(JS_ERR_INIT);
  }

  /* This reference makes sure async work is finished before */
  /* clean up gets triggered. */
  status = napi_create_reference(env, result, 0, &ntree->ref);

  if (status != napi_ok) {
    free(ntree);
    JS_THROW(JS_ERR_INIT);
  }

  return result;
}

NURKEL_EXEC(tree_open) {
  (void)env;
  nurkel_open_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->tree = urkel_open(worker->in_path);

  if (ntree->tree == NULL) {
    worker->err_res = urkel_errno;
    return;
  }

  urkel_root(ntree->tree, worker->out_hash);
  worker->success = true;
}

NURKEL_COMPLETE(tree_open) {
  nurkel_open_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;
  napi_value result;

  ntree->workers--;

  if (status != napi_ok || worker->success == false) {
    ntree->is_opening = false;

    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Urkel open failed.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    ntree->is_open = true;
    ntree->is_opening = false;

    NAPI_OK(napi_create_buffer_copy(env,
                                    URKEL_HASH_SIZE,
                                    worker->out_hash,
                                    NULL,
                                    &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  /* NOTE: We can clean this up because urkel_tree is not using it. */
  /* Internally it creates a copy in the store. */
  /* otherwise we would strcpy worker->path in nurkel_open_exec. */
  free(worker->in_path);
  free(worker);

  NAPI_OK(nurkel_close_try_close(env, ntree));
}

NURKEL_METHOD(tree_open) {
  napi_value result;
  napi_status status;
  nurkel_open_worker_t *worker = NULL;
  char *err;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();

  JS_ASSERT(!ntree->is_open && !ntree->is_opening, "Tree is already open.");
  JS_ASSERT(!ntree->is_closing, "Tree is closing.");

  worker = malloc(sizeof(nurkel_open_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);

  WORKER_INIT(worker);
  worker->ctx = ntree;
  worker->in_path = NULL;
  worker->in_path_len = 0;
  memset(worker->out_hash, 0, URKEL_HASH_SIZE);

  status = read_value_string_latin1(env,
                                    argv[1],
                                    &worker->in_path,
                                    &worker->in_path_len);
  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tree_open, worker, result);

  if (status != napi_ok) {
    err = JS_ERR_NODE;
    goto throw;
  }


  ntree->is_opening = true;
  ntree->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntree->is_opening = false;
    ntree->workers--;
    napi_delete_async_work(env, worker->work);
    err = JS_ERR_NODE;
    goto throw;
  }

  return result;
throw:
  if (worker != NULL) {
    free(worker->in_path);
    free(worker);
  }

  JS_THROW(err);
}

NURKEL_EXEC(tree_close) {
  (void)env;
  nurkel_close_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  urkel_close(ntree->tree);
  ntree->tree = NULL;
  worker->success = true;
}

/**
 * Same as above, this is called after _exec.
 */

NURKEL_COMPLETE(tree_close) {
  nurkel_close_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;
  napi_value result;

  ntree->is_closing = false;
  ntree->is_opening = false;
  ntree->is_open = false;
  ntree->should_close = false;
  ntree->close_worker = NULL;

  if (worker->deferred != NULL) {
    if (status != napi_ok || worker->success == false) {
      NAPI_OK(nurkel_create_error(env,
                                  worker->err_res,
                                  "Failed to close.",
                                  &result));
      NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    } else {
      NAPI_OK(napi_get_undefined(env, &result));
      NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
    }
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));

  if (worker->in_destroy || ntree->should_cleanup) {
    NAPI_OK(napi_delete_reference(env, ntree->ref));
    free(ntree);
  }
  free(worker);
}

/**
 * NAPI Call for closing tree.
 * This will wait indefintely if dependencies are not closed first.
 */
NURKEL_METHOD(tree_close) {
  napi_value result;
  napi_status status;
  nurkel_close_params_t params;

  NURKEL_ARGV(1);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  params.env = env;
  params.ctx = ntree;
  params.destroy = false;
  params.promise = true;
  params.promise_result = &result;

  status = nurkel_close_work(params);

  if (status != napi_ok)
    JS_THROW(JS_ERR_NODE);

  return result;
}

NURKEL_METHOD(tree_root_hash_sync) {
  napi_value result;
  uint8_t hash[URKEL_HASH_SIZE];

  NURKEL_ARGV(1);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  urkel_root(ntree->tree, hash);

  JS_ASSERT(napi_create_buffer_copy(env,
                                    URKEL_HASH_SIZE,
                                    hash,
                                    NULL,
                                    &result) == napi_ok, JS_ERR_NODE);

  return result;
}

NURKEL_EXEC(tree_root_hash) {
  (void)env;
  nurkel_root_hash_worker_t *worker = data;
  CHECK(worker != NULL);

  nurkel_tree_t *ntree = worker->ctx;
  urkel_root(ntree->tree, worker->out_hash);
  worker->success = true;
}

NURKEL_COMPLETE(tree_root_hash) {
  napi_value result;

  nurkel_root_hash_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->workers--;

  if (!worker->success || status != napi_ok) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to get root_hash.",
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
}

NURKEL_METHOD(tree_root_hash) {
  napi_value result;
  napi_status status;
  nurkel_root_hash_worker_t * worker = NULL;

  NURKEL_ARGV(1);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  worker = malloc(sizeof(nurkel_root_hash_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  worker->ctx = ntree;

  NURKEL_CREATE_ASYNC_WORK(tree_root_hash, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntree->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntree->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}

NURKEL_METHOD(tree_inject_sync) {
  napi_value result;
  napi_status status;
  uint8_t in_root[URKEL_HASH_SIZE];

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();
  NURKEL_JS_HASH_OK(argv[1], in_root);

  if (!urkel_inject(ntree->tree, in_root))
    JS_THROW_CODE(urkel_errno, "Failed to inject_sync.");

  JS_NAPI_OK(napi_get_undefined(env, &result), JS_ERR_NODE);
  return result;
}

NURKEL_EXEC(tree_inject) {
  (void)env;

  nurkel_inject_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  if (!urkel_inject(ntree->tree, worker->in_root)) {
    worker->err_res = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(tree_inject) {
  napi_value result;
  nurkel_inject_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->workers--;
  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to inject.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  NAPI_OK(nurkel_close_try_close(env, ntree));
  free(worker);
}

NURKEL_METHOD(tree_inject) {
  napi_value result;
  napi_status status;
  nurkel_inject_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  worker = malloc(sizeof(nurkel_inject_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntree;

  NURKEL_JS_HASH(argv[1], worker->in_root);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tree_inject, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntree->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntree->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}

NURKEL_METHOD(tree_get_sync) {
  napi_value result;
  napi_status status;
  uint8_t key[URKEL_HASH_SIZE];
  uint8_t value[URKEL_VALUE_SIZE];
  size_t value_len;
  int res;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();
  NURKEL_JS_HASH_OK(argv[1], key);

  res = urkel_get(ntree->tree, value, &value_len, key, NULL);

  if (res) {
    JS_NAPI_OK(napi_create_buffer_copy(env,
                                       value_len,
                                       value,
                                       NULL,
                                       &result), JS_ERR_NODE);

    return result;
  }

  if (urkel_errno == URKEL_ENOTFOUND) {
    JS_NAPI_OK(napi_get_null(env, &result), JS_ERR_NODE);
    return result;
  }

  JS_THROW_CODE(urkel_errno, "Failed to get.");
}

NURKEL_EXEC(tree_get) {
  (void)env;

  nurkel_get_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;
  int res = urkel_get(ntree->tree,
                      worker->out_value,
                      &worker->out_value_len,
                      worker->in_key,
                      NULL);

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

  worker->success = false;
  worker->err_res = urkel_errno;
}

NURKEL_COMPLETE(tree_get) {
  napi_value result;
  nurkel_get_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to get.",
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
  NAPI_OK(nurkel_close_try_close(env, ntree));
  free(worker);
}

NURKEL_METHOD(tree_get) {
  napi_value result;
  napi_status status;
  nurkel_get_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  worker = malloc(sizeof(nurkel_get_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntree;
  worker->out_has_key = false;

  NURKEL_JS_HASH(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tree_get, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  ntree->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntree->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}

NURKEL_METHOD(tree_has_sync) {
  napi_value result;
  napi_status status;
  uint8_t key[URKEL_HASH_SIZE];
  bool has_key = true;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();
  NURKEL_JS_HASH_OK(argv[1], key);

  if (!urkel_has(ntree->tree, key, NULL)) {
    if (urkel_errno != URKEL_ENOTFOUND)
      JS_THROW(urkel_errors[urkel_errno]);

    has_key = false;
  }

  JS_NAPI_OK(napi_get_boolean(env, has_key, &result), JS_ERR_NODE);
  return result;
}

NURKEL_EXEC(tree_has) {
  (void)env;

  nurkel_has_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  if (!urkel_has(ntree->tree, worker->in_key, NULL)) {
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

NURKEL_COMPLETE(tree_has) {
  napi_value result;
  nurkel_has_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;
  ntree->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to has.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_boolean(env, worker->out_has_key, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  NAPI_OK(nurkel_close_try_close(env, ntree));
  free(worker);
}

NURKEL_METHOD(tree_has) {
  napi_value result;
  napi_status status;
  nurkel_has_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  worker = malloc(sizeof(nurkel_has_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntree;

  NURKEL_JS_HASH(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tree_has, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  ntree->workers++;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntree->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}

NURKEL_METHOD(tree_insert_sync) {
  (void)env;
  (void)info;
  JS_THROW(JS_ERR_NOT_IMPL);
}

NURKEL_METHOD(tree_insert) {
  (void)env;
  (void)info;
  JS_THROW(JS_ERR_NOT_IMPL);
}

NURKEL_METHOD(tree_remove_sync) {
  (void)env;
  (void)info;
  JS_THROW(JS_ERR_NOT_SUPPORTED);
}

NURKEL_METHOD(tree_remove) {
  (void)env;
  (void)info;
  JS_THROW(JS_ERR_NOT_SUPPORTED);
}

NURKEL_METHOD(tree_prove_sync) {
  napi_value result;
  napi_status status;
  uint8_t in_key[URKEL_HASH_SIZE];
  uint8_t *out_proof_raw = NULL;
  size_t out_proof_len;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();
  NURKEL_JS_HASH_OK(argv[1], in_key);

  if (!urkel_prove(ntree->tree, &out_proof_raw, &out_proof_len, in_key, NULL))
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

NURKEL_EXEC(tree_prove) {
  (void)env;

  nurkel_prove_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  if (!urkel_prove(ntree->tree, &worker->out_proof, &worker->out_proof_len,
                      worker->in_key, NULL)) {
    worker->err_res = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(tree_prove) {
  napi_value result;
  nurkel_prove_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to prove.",
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
  NAPI_OK(nurkel_close_try_close(env, ntree));
  free(worker);
}

NURKEL_METHOD(tree_prove) {
  napi_value result;
  napi_status status;
  nurkel_prove_worker_t *worker;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  worker = malloc(sizeof(nurkel_prove_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntree;
  worker->out_proof = NULL;
  worker->out_proof_len = 0;

  NURKEL_JS_HASH(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(tree_prove, worker, result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  ntree->workers++;
  status = napi_queue_async_work(env, worker->work);
  if (status != napi_ok) {
    ntree->workers--;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  return result;
}

NURKEL_METHOD(verify_sync) {
  napi_value result_value;
  napi_value result_code;
  napi_value result;
  napi_status status;
  uint8_t root[URKEL_HASH_SIZE];
  uint8_t key[URKEL_HASH_SIZE];
  uint8_t value[URKEL_VALUE_SIZE];
  size_t value_len = 0;
  int exists = 0;
  uint8_t *proof;
  size_t proof_len;
  bool is_buffer;
  int res;

  NURKEL_ARGV(3);
  NURKEL_JS_HASH_OK(argv[0], root);
  NURKEL_JS_HASH_OK(argv[1], key);

  JS_NAPI_OK(napi_is_buffer(env, argv[2], &is_buffer), JS_ERR_ARG);
  JS_ASSERT(is_buffer, JS_ERR_ARG);
  JS_NAPI_OK(napi_get_buffer_info(env, argv[2], (void **)&proof, &proof_len), JS_ERR_ARG);
  JS_ASSERT(proof_len <= URKEL_PROOF_SIZE, JS_ERR_ARG);

  JS_NAPI_OK(napi_create_array_with_length(env, 2, &result), JS_ERR_NODE);

  res = urkel_verify(&exists, value, &value_len, proof, proof_len, key, root);

  if (!res) {
    JS_NAPI_OK(napi_create_int32(env, urkel_errno, &result_code), JS_ERR_NODE);
    JS_NAPI_OK(napi_get_null(env, &result_value), JS_ERR_NODE);

    JS_NAPI_OK(napi_set_element(env, result, 0, result_code), JS_ERR_NODE);
    JS_NAPI_OK(napi_set_element(env, result, 1, result_value), JS_ERR_NODE);
    return result;
  }

  if (exists) {
    JS_NAPI_OK(napi_create_buffer_copy(env,
                                       value_len,
                                       value,
                                       NULL,
                                       &result_value), JS_ERR_NODE);
  } else {
    JS_NAPI_OK(napi_get_null(env, &result_value), JS_ERR_NODE);
  }

  JS_NAPI_OK(napi_create_int32(env, URKEL_OK, &result_code), JS_ERR_NODE);
  JS_NAPI_OK(napi_set_element(env, result, 0, result_code), JS_ERR_NODE);
  JS_NAPI_OK(napi_set_element(env, result, 1, result_value), JS_ERR_NODE);

  return result;
}

NURKEL_EXEC(verify) {
  (void)env;

  nurkel_verify_worker_t *worker = data;

  if (!urkel_verify(&worker->out_exists,
                    worker->out_value,
                    &worker->out_value_len,
                    worker->in_proof,
                    worker->in_proof_len,
                    worker->in_key,
                    worker->in_root)) {
    worker->success = false;
    worker->err_res = urkel_errno;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(verify) {
  napi_value result;
  napi_value result_code;
  napi_value result_value;
  nurkel_verify_worker_t *worker = data;

  if (status != napi_ok) {
    NAPI_OK(nurkel_create_error(env,
                                URKEL_UNKNOWN,
                                "Failed to verify.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else if (worker->success == false) {
    NAPI_OK(napi_create_array_with_length(env, 2, &result));
    NAPI_OK(napi_create_int32(env, worker->err_res, &result_code));
    NAPI_OK(napi_get_null(env, &result_value));
    NAPI_OK(napi_set_element(env, result, 0, result_code));
    NAPI_OK(napi_set_element(env, result, 1, result_value));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_create_array_with_length(env, 2, &result));

    if (worker->out_exists) {
      NAPI_OK(napi_create_buffer_copy(env,
                                      worker->out_value_len,
                                      worker->out_value,
                                      NULL,
                                      &result_value));
    } else {
      NAPI_OK(napi_get_null(env, &result_value));
    }

    NAPI_OK(napi_create_int32(env, URKEL_OK, &result_code));
    NAPI_OK(napi_set_element(env, result, 0, result_code));
    NAPI_OK(napi_set_element(env, result, 1, result_value));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker->in_proof);
  free(worker);
}

NURKEL_METHOD(verify) {
  napi_value result;
  napi_status status;
  nurkel_verify_worker_t *worker;
  size_t proof_len;
  char *err;
  bool is_buffer;

  NURKEL_ARGV(3);

  JS_NAPI_OK(napi_is_buffer(env, argv[2], &is_buffer), JS_ERR_ARG);
  JS_ASSERT(is_buffer, JS_ERR_ARG);
  JS_NAPI_OK(napi_get_buffer_info(env, argv[2], NULL, &proof_len), JS_ERR_ARG);

  worker = malloc(sizeof(nurkel_verify_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);

  worker->in_proof = malloc(proof_len);
  if (worker->in_proof == NULL) {
    free(worker);
    JS_THROW(JS_ERR_ALLOC);
  }

  NURKEL_JS_HASH(argv[0], worker->in_root);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_ARG);

  NURKEL_JS_HASH(argv[1], worker->in_key);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_ARG);

  status = nurkel_get_buffer_copy(env,
                                  argv[2],
                                  worker->in_proof,
                                  &worker->in_proof_len,
                                  proof_len,
                                  false);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_ARG);

  NURKEL_CREATE_ASYNC_WORK(verify, worker, result);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    JS_ASSERT_GOTO_THROW(false, JS_ERR_NODE);
  }

  return result;

throw:
  free(worker->in_proof);
  free(worker);
  JS_THROW(err);
}

NURKEL_METHOD(destroy_sync) {
  napi_value result;
  char *path = NULL;
  size_t path_len = 0;

  NURKEL_ARGV(1);

  JS_NAPI_OK(read_value_string_latin1(env, argv[0], &path, &path_len),
             JS_ERR_ARG);

  if (!urkel_destroy(path)) {
    free(path);
    JS_THROW_CODE(urkel_errno, JS_ERR_URKEL_DESTROY);
  }

  free(path);
  JS_NAPI_OK(napi_get_undefined(env, &result), JS_ERR_NODE);
  return result;
}

NURKEL_EXEC(destroy) {
  (void)env;
  nurkel_destroy_worker_t *worker = data;

  if (!urkel_destroy(worker->in_path)) {
    worker->err_res = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(destroy) {
  napi_value result;
  nurkel_destroy_worker_t *worker = data;

  if (status != napi_ok || worker->err_res != 0) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                JS_ERR_URKEL_DESTROY,
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker->in_path);
  free(worker);
}

NURKEL_METHOD(destroy) {
  napi_value result;
  napi_status status;
  nurkel_destroy_worker_t *worker;
  char *err;

  NURKEL_ARGV(1);

  worker = malloc(sizeof(nurkel_destroy_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->in_path = NULL;
  worker->in_path_len = 0;

  status = read_value_string_latin1(env,
                                    argv[0],
                                    &worker->in_path,
                                    &worker->in_path_len);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_ARG);

  NURKEL_CREATE_ASYNC_WORK(destroy, worker, result);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    JS_ASSERT_GOTO_THROW(false, JS_ERR_NODE);
  }

  return result;
throw:
  if (worker->in_path != NULL)
    free(worker->in_path);

  free(worker);
  JS_THROW(err);
}

NURKEL_METHOD(hash_sync) {
  napi_value result;
  uint8_t out_hash[URKEL_HASH_SIZE];
  bool is_buffer;
  uint8_t *in_data = NULL;
  size_t in_data_len;

  NURKEL_ARGV(1);

  JS_NAPI_OK(napi_is_buffer(env, argv[0], &is_buffer), JS_ERR_ARG);
  JS_ASSERT(is_buffer == true, JS_ERR_ARG);
  JS_NAPI_OK(napi_get_buffer_info(env,
                                  argv[0],
                                  (void **)&in_data,
                                  &in_data_len), JS_ERR_ARG);

  urkel_hash(out_hash, in_data, in_data_len);

  JS_NAPI_OK(napi_create_buffer_copy(env,
                                     URKEL_HASH_SIZE,
                                     out_hash,
                                     NULL,
                                     &result), JS_ERR_NODE);

  return result;
}

NURKEL_EXEC(hash) {
  (void)env;

  nurkel_hash_worker_t *worker = data;

  urkel_hash(worker->out_hash, worker->in_data, worker->in_data_len);
  worker->success = true;
}

NURKEL_COMPLETE(hash) {
  napi_value result;
  nurkel_hash_worker_t *worker = data;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to hash.",
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

  NAPI_OK(napi_delete_reference(env, worker->ref));
  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker);
}

NURKEL_METHOD(hash) {
  napi_value result;
  napi_status status;
  bool is_buffer;
  char *err;
  nurkel_hash_worker_t *worker;

  NURKEL_ARGV(1);

  JS_NAPI_OK(napi_is_buffer(env, argv[0], &is_buffer), JS_ERR_ARG);
  JS_ASSERT(is_buffer == true, JS_ERR_ARG);

  worker = malloc(sizeof(nurkel_hash_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);

  /* Move instead of copy */
  status = napi_get_buffer_info(env,
                                argv[0],
                                (void **)&worker->in_data,
                                &worker->in_data_len);

  if (status != napi_ok) {
    err = JS_ERR_ARG;
    goto throw;
  }

  /* Make sure buffer does not get freed while we are working with it. */
  status = napi_create_reference(env, argv[0], 1, &worker->ref);

  if (status != napi_ok) {
    err = JS_ERR_NODE;
    goto throw;
  }

  NURKEL_CREATE_ASYNC_WORK(hash, worker, result);

  if (status != napi_ok) {
    err = JS_ERR_NODE;
    goto throw;
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    err = JS_ERR_NODE;
    goto throw;
  }

  return result;
throw:
  if (worker->work != NULL)
    napi_delete_async_work(env, worker->work);

  if (worker->ref != NULL)
    napi_delete_reference(env, worker->ref);

  free(worker);
  JS_THROW(err);
}

NURKEL_METHOD(compact_sync) {
  napi_value result;
  napi_status status;
  char *src = NULL, *dst = NULL;
  size_t src_len, dst_len;
  uint8_t root[URKEL_HASH_SIZE];
  uint8_t *root_ptr = NULL;
  napi_valuetype type;

  NURKEL_ARGV(3);

  JS_NAPI_OK(napi_typeof(env, argv[2], &type), JS_ERR_ARG);

  if (type != napi_null && type != napi_undefined) {
    NURKEL_JS_HASH_OK(argv[2], root);
    root_ptr = root;
  }

  JS_NAPI_OK(napi_get_undefined(env, &result), JS_ERR_NODE);
  JS_NAPI_OK(read_value_string_latin1(env,
                                      argv[0],
                                      &src,
                                      &src_len), JS_ERR_ARG);

  status = read_value_string_latin1(env,
                                    argv[1],
                                    &dst,
                                    &dst_len);

  if (status != napi_ok) {
    free(src);
    JS_THROW(JS_ERR_ARG);
  }

  if (!urkel_compact(dst, src, root_ptr)) {
    free(src);
    free(dst);
    JS_THROW_CODE(urkel_errno, "Failed to compact_sync.");
  }

  return result;
}

NURKEL_EXEC(compact) {
  (void)env;

  nurkel_compact_worker_t *worker = data;

  if (!urkel_compact(worker->in_dst, worker->in_src, worker->in_root)) {
    worker->success = false;
    worker->err_res = urkel_errno;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(compact) {
  napi_value result;
  nurkel_compact_worker_t *worker = data;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to compact.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker->in_root);
  free(worker->in_dst);
  free(worker->in_src);
  free(worker);
}

NURKEL_METHOD(compact) {
  napi_value result;
  napi_status status;
  napi_valuetype type;
  char *err;
  nurkel_compact_worker_t *worker = NULL;
  uint8_t *in_root = NULL;

  NURKEL_ARGV(3);

  JS_NAPI_OK(napi_typeof(env, argv[2], &type), JS_ERR_ARG);

  worker = malloc(sizeof(nurkel_compact_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->in_root = NULL;

  if (type != napi_null && type != napi_undefined) {
    in_root = malloc(URKEL_HASH_SIZE);
    JS_ASSERT_GOTO_THROW(in_root != NULL, JS_ERR_ALLOC);

    NURKEL_JS_HASH_OK(argv[2], in_root);
    worker->in_root = in_root;
  }

  status = read_value_string_latin1(env,
                                    argv[0],
                                    &worker->in_src,
                                    &worker->in_src_len);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_ARG);

  status = read_value_string_latin1(env,
                                    argv[1],
                                    &worker->in_dst,
                                    &worker->in_dst_len);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_ARG);

  NURKEL_CREATE_ASYNC_WORK(compact, worker, result);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    JS_ASSERT_GOTO_THROW(false, JS_ERR_NODE);
  }

  return result;

throw:
  if (in_root != NULL)
    free(in_root);

  free(worker);

  JS_THROW(err);
}

NURKEL_METHOD(stat_sync) {
  napi_value result, result_size, result_files;
  napi_status status;
  char *err;
  char *in_prefix;
  size_t in_prefix_len;
  urkel_tree_stat_t st = {0};

  NURKEL_ARGV(1);

  JS_NAPI_OK(read_value_string_latin1(env,
                                      argv[0],
                                      &in_prefix,
                                      &in_prefix_len), JS_ERR_ARG);

  if (!urkel_stat(in_prefix, &st)) {
    free(in_prefix);
    JS_THROW(urkel_errors[urkel_errno]);
  }

  status = napi_create_object(env, &result);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  status = napi_create_int64(env, st.size, &result_size);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  status = napi_create_int64(env, st.files, &result_files);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  status = napi_set_named_property(env, result, "size", result_size);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  status = napi_set_named_property(env, result, "files", result_files);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  return result;

throw:
  free(in_prefix);
  JS_THROW(err);
}

NURKEL_EXEC(stat) {
  (void)env;

  nurkel_stat_worker_t *worker = data;

  if (!urkel_stat(worker->in_prefix, &worker->out_st)) {
    worker->err_res = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(stat) {
  napi_value result, result_size, result_files;
  nurkel_stat_worker_t *worker = data;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->err_res,
                                "Failed to stat.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_create_object(env, &result));
    NAPI_OK(napi_create_int64(env, worker->out_st.size, &result_size));
    NAPI_OK(napi_create_int64(env, worker->out_st.files, &result_files));
    NAPI_OK(napi_set_named_property(env, result, "size", result_size));
    NAPI_OK(napi_set_named_property(env, result, "files", result_files));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker->in_prefix);
  free(worker);
}

NURKEL_METHOD(stat) {
  napi_value result;
  napi_status status;
  nurkel_stat_worker_t *worker;
  char *err;

  NURKEL_ARGV(1);

  worker = malloc(sizeof(nurkel_stat_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->out_st.files = 0;
  worker->out_st.size = 0;

  status = read_value_string_latin1(env,
                                    argv[0],
                                    &worker->in_prefix,
                                    &worker->in_prefix_len);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_ARG);

  NURKEL_CREATE_ASYNC_WORK(stat, worker, result);
  JS_ASSERT_GOTO_THROW(status == napi_ok, JS_ERR_NODE);

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    JS_ASSERT_GOTO_THROW(false, JS_ERR_NODE);
  }

  return result;

throw:
  free(worker->in_prefix);
  free(worker);
  JS_THROW(err);
}
