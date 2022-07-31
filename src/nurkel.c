/**
 * nurkel.c - native bindings to liburkel
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/liburkel
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <urkel.h>
#include <node_api.h>

#define URKEL_HASH_SIZE 32

#define JS_ERR_INIT "Failed to initialize."
#define JS_ERR_NOT_IMPL "Not implemented."
#define JS_ERR_ARG "Invalid argument."
#define JS_ERR_ALLOC "Allocation failed."
#define JS_ERR_NODE "Node internal error."

#define JS_ERR_TREE_OPEN "Tree is already open."
#define JS_ERR_TREE_CLOSED "Tree is closed."
#define JS_ERR_TREE_NOTREADY "Tree is not ready."

#define JS_ERR_TX_OPEN "Transaction is already open."
#define JS_ERR_TX_CLOSED "Transaction is closed."
#define JS_ERR_TX_NOTREADY "Transaction is not ready."
#define JS_ERR_TX_CLOSE_FAIL "Failed to start close worker."

#define JS_ERR_URKEL_UNKNOWN "Unknown urkel error."
#define JS_ERR_URKEL_OPEN "Urkel open failed."
#define JS_ERR_URKEL_CLOSE "Urkel close failed."

#define ASYNC_OPEN "nurkel_open"
#define ASYNC_CLOSE "nurkel_close"
#define ASYNC_DESTROY "nurkel_destroy"
#define ASYNC_ROOT_HASH "nurkel_root_hash"

#define ASYNC_TX_OPEN "nurkel_tx_open"
#define ASYNC_TX_CLOSE "nurkel_tx_close"
#define ASYNC_TX_DESTROY "nurkel_tx_destroy"
#define ASYNC_TX_ROOT_HASH "nurkel_tx_root_hash"

#define CHECK(expr) do {                           \
  if (!(expr))                                     \
    nurkel_assert_fail(__FILE__, __LINE__, #expr); \
} while (0)

#define NAPI_OK(expr) do {                         \
  if ((expr) != napi_ok)                           \
    nurkel_assert_fail(__FILE__, __LINE__, #expr); \
} while(0)

#define JS_THROW(msg) do {                              \
  CHECK(napi_throw_error(env, NULL, (msg)) == napi_ok); \
  return NULL;                                          \
} while (0)

#define JS_ASSERT(cond, msg) if (!(cond)) JS_THROW(msg)

/*
 * Urkel errors
 */

/* Errnos start with 1, 0 = everything's ok. */
static char *urkel_errors[] = {
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

static const int urkel_errors_len = 13;

/*
 * NAPI related macros.
 */

#define NURKEL_METHOD(name)                             \
static napi_value                                       \
nurkel_ ## name (napi_env env, napi_callback_info info)

/* EXEC refers to the code that will be executed in the workers. */
#define NURKEL_EXEC_DECL(name)                   \
  static void                                    \
  nurkel_ ## name ## _exec(napi_env, void *data)

#define NURKEL_EXEC_NAME(name) nurkel_ ## name ## _exec

#define NURKEL_EXEC(name)                          \
static void                                        \
nurkel_ ## name ## _exec(napi_env env, void *data)

/* Complete refers to callbacks that are called from the worker thread pool. */
#define NURKEL_COMPLETE_DECL(name)                                           \
  static void                                                                \
  nurkel_ ## name ## _complete(napi_env env, napi_status status, void *data)

#define NURKEL_COMPLETE_NAME(name) nurkel_ ## name ## _complete

#define NURKEL_COMPLETE(name)                                              \
static void                                                                \
nurkel_ ## name ## _complete(napi_env env, napi_status status, void *data)

#define NURKEL_ARGV(n)                                                       \
  size_t argc = n;                                                           \
  napi_value argv[n];                                                        \
  JS_ASSERT(napi_get_cb_info(env, info, &argc, argv, NULL, NULL) == napi_ok, \
            JS_ERR_ARG);                                                     \
  JS_ASSERT(argc == n, JS_ERR_ARG)

#define NURKEL_TREE_CONTEXT()                                                  \
  nurkel_tree_t *ntree = NULL;                                                 \
  JS_ASSERT(napi_get_value_external(env, argv[0], (void **)&ntree) == napi_ok, \
            JS_ERR_ARG);                                                       \
  JS_ASSERT(ntree != NULL, JS_ERR_ARG)

#define NURKEL_TX_CONTEXT()                                                  \
  nurkel_tx_t *ntx = NULL;                                                   \
  nurkel_tree_t *ntree = NULL;                                               \
  JS_ASSERT(napi_get_value_external(env, argv[0], (void **)&ntx) == napi_ok, \
            JS_ERR_ARG);                                                     \
  JS_ASSERT(ntx != NULL, JS_ERR_ARG);                                        \
  ntree = ntx->ntree

#define NURKEL_TREE_READY()                                 \
  JS_ASSERT(nurkel_tree_ready(ntree), JS_ERR_TREE_NOTREADY)

#define NURKEL_TX_READY()                                   \
  JS_ASSERT(nurkel_tx_ready(ntx), JS_ERR_TX_NOTREADY);      \
  JS_ASSERT(nurkel_tree_ready(ntree), JS_ERR_TREE_NOTREADY)

#define NURKEL_JS_WORKNAME(name)                                         \
  JS_ASSERT(napi_create_string_latin1(env,                               \
                                     name,                               \
                                     NAPI_AUTO_LENGTH,                   \
                                     &workname) == napi_ok, JS_ERR_NODE)

/*
 * NAPI Context wrappers for the urkel.
 */

typedef struct nurkel_tx_s nurkel_tx_t;

typedef struct nurkel_tx_entry_s {
  nurkel_tx_t *ntx;

  struct nurkel_tx_entry_s *prev;
  struct nurkel_tx_entry_s *next;
} nurkel_tx_entry_t;


typedef struct nurkel_tree_s {
  urkel_t *tree;
  unsigned char root[URKEL_HASH_SIZE];
  napi_ref ref;
  uint32_t workers;
  void *close_worker;

  uint32_t tx_len;
  nurkel_tx_entry_t *tx_head;

  bool is_open;
  bool is_opening;
  bool is_closing;
  bool should_close;
  bool should_cleanup;
} nurkel_tree_t;

typedef struct nurkel_tx_s {
  nurkel_tree_t *ntree;
  urkel_tx_t *tx;
  nurkel_tx_entry_t *entry;
  uint32_t workers;
  void *close_worker;
  unsigned char root[URKEL_HASH_SIZE];
  bool is_open;
  bool is_opening;
  bool is_closing;
  bool should_close;
  bool should_cleanup;
} nurkel_tx_t;

/*
 * Worker setup
 */

#define WORKER_BASE_PROPS \
  void *ctx;              \
  int errno;              \
  bool success;           \
  napi_deferred deferred; \
  napi_async_work work;

#define WORKER_INIT(worker) do { \
  worker->errno = 0;             \
  worker->success = false;       \
  worker->ctx = NULL;            \
  worker->deferred = NULL;       \
  worker->work = NULL;           \
} while(0)

typedef struct nurkel_open_worker_s {
  WORKER_BASE_PROPS
  char *path;
  size_t path_len;
  uint8_t hash[URKEL_HASH_SIZE];
} nurkel_open_worker_t;

typedef struct nurkel_close_worker_s {
  WORKER_BASE_PROPS
  bool destroy;
} nurkel_close_worker_t;

typedef struct nurkel_tx_open_worker_s {
  WORKER_BASE_PROPS
} nurkel_tx_open_worker_t;

typedef struct nurkel_tx_close_worker_s {
  WORKER_BASE_PROPS
  bool destroy;
} nurkel_tx_close_worker_t;

typedef struct nurkel_tx_root_worker_s {
  uint8_t hash[URKEL_HASH_SIZE];
  WORKER_BASE_PROPS
} nurkel_tx_root_hash_worker_t;

typedef struct nurkel_root_hash_worker_s {
  uint8_t hash[URKEL_HASH_SIZE];
  WORKER_BASE_PROPS
} nurkel_root_hash_worker_t;

#undef WORKER_BASE_PROPS

#define WORKER_CLOSE_PARAMS   \
  napi_env env;               \
  void *ctx;                  \
  napi_value *promise_result; \
  bool promise;               \
  bool destroy;               \

typedef struct nurkel_close_params_s {
  WORKER_CLOSE_PARAMS
} nurkel_close_params_t;

typedef struct nurkel_tx_close_params_s {
  WORKER_CLOSE_PARAMS
} nurkel_tx_close_params_t;

/*
 * Helpers
 */

void
nurkel_assert_fail(const char *file, int line, const char *expr) {
  fprintf(stderr, "%s:%d: Assertion `%s' failed.\n", file, line, expr);
  fflush(stderr);
  abort();
}

napi_status
read_value_string_latin1(napi_env env, napi_value value,
                         char **str, size_t *length) {
  char *buf;
  size_t buflen;
  napi_status status;

  status = napi_get_value_string_latin1(env, value, NULL, 0, &buflen);

  if (status != napi_ok)
    return status;

  buf = malloc(buflen + 1);

  if (buf == NULL)
    return napi_generic_failure;

  status = napi_get_value_string_latin1(env,
                                        value,
                                        buf,
                                        buflen + 1,
                                        length);

  if (status != napi_ok) {
    free(buf);
    return status;
  }

  CHECK(*length == buflen);

  *str = buf;

  return napi_ok;
}

/*
 * Context wrapper init/deinit helpers.
 */

static void
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

  memset(ntree->root, 0, URKEL_HASH_SIZE);
}

static inline bool
nurkel_tree_ready(nurkel_tree_t *ntree) {
  return ntree->is_open && !ntree->is_closing
      && !ntree->is_opening && !ntree->should_close;
}

static void
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
  memset(ntx->root, 0, URKEL_HASH_SIZE);
}

static inline bool
nurkel_tx_ready(nurkel_tx_t *ntx) {
  return ntx->is_open && !ntx->is_closing
    && !ntx->is_opening && !ntx->should_close;
}

static void
nurkel_register_tx(nurkel_tx_t *ntx) {
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

static void
nurkel_unregister_tx(nurkel_tx_t *ntx) {
  nurkel_tree_t *ntree = ntx->ntree;
  nurkel_tx_entry_t *entry = ntx->entry;

  CHECK(entry != NULL);
  CHECK(ntree->tx_len > 0);

  ntree->tx_len--;
  ntx->entry = NULL;

  if (ntree->tx_len == 0)
    ntree->tx_head = NULL;

  if (entry->prev != NULL)
    entry->prev->next = entry->next;

  if (entry->next != NULL)
    entry->next->prev = entry->prev;

  free(entry);
}

/*
 * NAPI for urkel.
 *
 * Note that functions ending with _exec are all executed in
 * the worker pool thread. They should not access/call JS
 * or napi_env. If it's absolutely necessary check `napi_thread_safe_function`.
 */

/*
 * Forward declarations for necessary stuff.
 */


NURKEL_EXEC_DECL(close);
NURKEL_COMPLETE_DECL(close);

static napi_status
nurkel_close_work(nurkel_close_params_t *params);

static napi_status
nurkel_tx_close_work(nurkel_tx_close_params_t *params);

static inline napi_status
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

static inline napi_status
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

/*
 * This is called when tree variable goes out of scope and gets GCed. We need to
 * make sure on the JS side to always have Tree in scope until it is being
 * used by iterators and batches. So only time we go out of the scope
 * if everything else went out of scope as well. So on GC we can freely
 * clean up.
 */
static void
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

    NAPI_OK(nurkel_close_work(&params));
    return;
  }

  if (ntree->should_close) {
    nurkel_close_worker_t *worker = ntree->close_worker;
    worker->destroy = true;
    return;
  }

  free(ntree);
}

NURKEL_METHOD(init) {
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

/*
 * Tree Open and related.
 */

NURKEL_EXEC(open) {
  (void)env;
  nurkel_open_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->tree = urkel_open(worker->path);

  if (ntree->tree == NULL) {
    worker->errno = urkel_errno;
    return;
  }

  urkel_root(ntree->tree, worker->hash);
  worker->success = true;
}

/**
 * Done opening tree.
 * Nothing else should have happened before this function because of the
 * `is_opening` guard.
 */

NURKEL_COMPLETE(open) {
  nurkel_open_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;
  napi_value result, msg, code;

  ntree->workers--;

  if (status != napi_ok || worker->success == false) {
    ntree->is_opening = false;

    CHECK(worker->errno > 0 && worker->errno <= urkel_errors_len);
    NAPI_OK(napi_create_string_latin1(env,
                                      urkel_errors[worker->errno - 1],
                                      NAPI_AUTO_LENGTH,
                                      &code));
    NAPI_OK(napi_create_string_latin1(env,
                                      JS_ERR_URKEL_OPEN,
                                      NAPI_AUTO_LENGTH,
                                      &msg));
    NAPI_OK(napi_create_error(env, code, msg, &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    goto cleanup;
  }

  ntree->is_open = true;
  ntree->is_opening = false;
  memcpy(ntree->root, worker->hash, URKEL_HASH_SIZE);

  NAPI_OK(napi_create_buffer_copy(env,
                                  URKEL_HASH_SIZE,
                                  ntree->root,
                                  NULL,
                                  &result));
  NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));

cleanup:
  NAPI_OK(napi_delete_async_work(env, worker->work));
  /* NOTE: We can clean this up because urkel_tree is not using it. */
  /* Internally it creates a copy in the store. */
  /* otherwise we would strcpy worker->path in nurkel_open_exec. */
  free(worker->path);
  free(worker);

  NAPI_OK(nurkel_close_try_close(env, ntree));
}

NURKEL_METHOD(open) {
  napi_value result, workname;
  napi_status status;
  nurkel_open_worker_t *worker = NULL;
  char *err;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();

  NURKEL_JS_WORKNAME(ASYNC_OPEN);
  JS_ASSERT(!ntree->is_open && !ntree->is_opening, JS_ERR_TREE_OPEN);
  JS_ASSERT(!ntree->is_closing, JS_ERR_TREE_CLOSED);

  worker = malloc(sizeof(nurkel_open_worker_t));
  if (worker == NULL) {
    err = JS_ERR_ALLOC;
    goto throw;
  }

  WORKER_INIT(worker);
  worker->ctx = ntree;
  worker->path = NULL;
  worker->path_len = 0;
  memset(worker->hash, 0, URKEL_HASH_SIZE);

  status = read_value_string_latin1(env,
                                    argv[1],
                                    &worker->path,
                                    &worker->path_len);
  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  status = napi_create_promise(env, &worker->deferred, &result);
  if (status != napi_ok) {
    err = JS_ERR_NODE;
    goto throw;
  }

  status = napi_create_async_work(env,
                                  NULL,
                                  workname,
                                  NURKEL_EXEC_NAME(open),
                                  NURKEL_COMPLETE_NAME(open),
                                  worker,
                                  &worker->work);

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
    free(worker->path);
    free(worker);
  }

  JS_THROW(err);
}

/*
 * Tree Close
 */

static void
nurkel_close_work_txs(nurkel_close_params_t *params) {
  napi_env env = params->env;
  nurkel_tree_t *ntree = params->ctx;
  nurkel_tx_entry_t *head = ntree->tx_head;
  nurkel_tx_close_params_t tx_params = {
    .env = env,
    .destroy = params->destroy,
    .promise = false,
    .promise_result = NULL
  };

  while (head != NULL) {
    tx_params.ctx = head->ntx;
    nurkel_tx_close_work(&tx_params);
    head = head->next;
  }
}

static napi_status
nurkel_close_work(nurkel_close_params_t *params) {
  napi_value workname;
  napi_status status;
  nurkel_close_worker_t *worker;
  nurkel_tree_t *ntree = params->ctx;

  if (ntree->is_closing || ntree->should_close)
    return napi_ok;

  status = napi_create_string_latin1(params->env,
                                     ASYNC_CLOSE,
                                     NAPI_AUTO_LENGTH,
                                     &workname);
  if (status != napi_ok)
    return status;

  worker = malloc(sizeof(nurkel_close_worker_t));
  if (worker == NULL)
    return napi_generic_failure;

  WORKER_INIT(worker);
  worker->ctx = ntree;
  worker->destroy = params->destroy;

  if (params->promise) {
    status = napi_create_promise(params->env,
                                 &worker->deferred,
                                 params->promise_result);
    if (status != napi_ok) {
      free(worker);
      return status;
    }
  }

  status = napi_create_async_work(params->env,
                                  NULL,
                                  workname,
                                  NURKEL_EXEC_NAME(close),
                                  NURKEL_COMPLETE_NAME(close),
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
  status = napi_queue_async_work(params->env, worker->work);

  if (status != napi_ok) {
    ntree->is_closing = false;
    napi_delete_async_work(params->env, worker->work);
    free(worker);
    return napi_generic_failure;
  }

  return napi_ok;
}


/*
 * This only needs to be called when everything related
 * to database has been cleaned up, because this will
 * free the database.
 */

NURKEL_EXEC(close) {
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

NURKEL_COMPLETE(close) {
  nurkel_close_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;
  napi_value result;

  ntree->is_closing = false;
  ntree->is_opening = false;
  ntree->is_open = false;
  ntree->should_close = false;
  ntree->close_worker = NULL;

  NAPI_OK(napi_delete_async_work(env, worker->work));

  if (worker->deferred != NULL) {
    NAPI_OK(napi_get_undefined(env, &result));

    if (status != napi_ok)
      NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    else
      NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  if (worker->destroy || ntree->should_cleanup)
    free(ntree);
  free(worker);
}

/**
 * NAPI Call for closing tree.
 * This will wait indefintely if dependencies are not closed first.
 */
NURKEL_METHOD(close) {
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

  status = nurkel_close_work(&params);

  if (status != napi_ok)
    JS_THROW(JS_ERR_NODE);

  return result;
}

NURKEL_METHOD(root_hash_sync) {
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

NURKEL_EXEC(root_hash) {
  (void)env;
  nurkel_root_hash_worker_t *worker = data;
  CHECK(worker != NULL);

  nurkel_tree_t *ntree = worker->ctx;
  urkel_root(ntree->tree, worker->hash);
  worker->success = true;
}

NURKEL_COMPLETE(root_hash) {
  napi_value result;

  nurkel_root_hash_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->workers--;

  if (!worker->success || status != napi_ok) {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    free(worker);
    return;
  }

  NAPI_OK(napi_create_buffer_copy(env,
                                  URKEL_HASH_SIZE,
                                  worker->hash,
                                  NULL,
                                  &result));
  NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  free(worker);
}

NURKEL_METHOD(root_hash) {
  napi_value result, workname;
  napi_status status;
  nurkel_root_hash_worker_t * worker = NULL;

  NURKEL_ARGV(1);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  NURKEL_JS_WORKNAME(ASYNC_ROOT_HASH);

  worker = malloc(sizeof(nurkel_root_hash_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  worker->ctx = ntree;

  status = napi_create_promise(env, &worker->deferred, &result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_create_async_work(env,
                                  NULL,
                                  workname,
                                  NURKEL_EXEC_NAME(root_hash),
                                  NURKEL_COMPLETE_NAME(root_hash),
                                  worker,
                                  &worker->work);

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

/*
 * Transaction related
 */

NURKEL_EXEC_DECL(tx_close);
NURKEL_COMPLETE_DECL(tx_close);

static void
nurkel_tx_destroy(napi_env env, void *data, void *hint) {
  (void)hint;

  if (!data)
    return;

  nurkel_tx_t *ntx = data;
  nurkel_tree_t *ntree = ntx->ntree;

  /* We only allow tree to go out of scope after all txs go out of scope. */
  NAPI_OK(napi_reference_unref(env, ntree->ref, NULL));

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

    NAPI_OK(nurkel_tx_close_work(&params));
    return;
  }

  if (ntx->should_close) {
    nurkel_tx_close_worker_t *worker = ntx->close_worker;
    worker->destroy = true;
    return;
  }

  nurkel_close_try_close(env, ntree);
  free(ntx);
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

  ntx->tx = urkel_tx_create(ntree->tree, ntx->root);
  worker->success = true;
}

NURKEL_COMPLETE(tx_open) {
  (void)status;

  nurkel_tx_open_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  nurkel_tree_t *ntree = ntx->ntree;
  napi_value result;

  ntx->workers--;

  ntx->is_open = true;
  ntx->is_opening = false;
  NAPI_OK(napi_get_undefined(env, &result));
  NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));

  /* At this point, tree could have started closing and waiting for tx ? */
  /* We are closing right away.. Tree must have said so.. */
  if (ntx->should_close) {
    /* Sanity check */
    CHECK(ntree->should_close == true);
    nurkel_tx_close_try_close(env, ntx);
  }

  free(worker);
}

NURKEL_METHOD(tx_open) {
  napi_value result, workname;
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

  JS_ASSERT(!ntx->is_open && !ntx->is_opening && !ntx->is_closing,
            JS_ERR_TX_OPEN);
  JS_ASSERT(nurkel_tree_ready(ntx->ntree), JS_ERR_TREE_CLOSED);
  ntree = ntx->ntree;

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
    memcpy(ntx->root, buffer, URKEL_HASH_SIZE);
  } else {
    memcpy(ntx->root, &ntree->root, URKEL_HASH_SIZE);
  }

  JS_ASSERT(napi_create_string_latin1(env,
                                      ASYNC_TX_OPEN,
                                      NAPI_AUTO_LENGTH,
                                      &workname) == napi_ok, JS_ERR_NODE);

  worker = malloc(sizeof(nurkel_tx_open_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  status = napi_create_promise(env, &worker->deferred, &result);
  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_create_async_work(env,
                                  NULL,
                                  workname,
                                  NURKEL_EXEC_NAME(tx_open),
                                  NURKEL_COMPLETE_NAME(tx_open),
                                  worker,
                                  &worker->work);

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

static napi_status
nurkel_tx_close_work(nurkel_tx_close_params_t *params) {
  napi_value workname;
  napi_status status;
  nurkel_tx_close_worker_t *worker;
  nurkel_tx_t *ntx = params->ctx;

  if (ntx->is_closing || ntx->should_close)
    return napi_ok;

  status = napi_create_string_latin1(params->env,
                                     ASYNC_TX_CLOSE,
                                     NAPI_AUTO_LENGTH,
                                     &workname);

  if (status != napi_ok)
    return status;

  worker = malloc(sizeof(nurkel_tx_close_worker_t));
  if (worker == NULL)
    return napi_generic_failure;

  WORKER_INIT(worker);
  worker->ctx = ntx;
  worker->destroy = params->destroy;

  if (params->promise) {
    status = napi_create_promise(params->env,
                                 &worker->deferred,
                                 params->promise_result);

    if (status != napi_ok) {
      free(worker);
      return status;
    }
  }

  status = napi_create_async_work(params->env,
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
  status = napi_queue_async_work(params->env, worker->work);

  if (status != napi_ok) {
    free(worker);
    return napi_generic_failure;
  }

  return napi_ok;
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

  NAPI_OK(napi_delete_async_work(env, worker->work));

  if (worker->deferred != NULL) {
    NAPI_OK(napi_get_undefined(env, &result));

    if (status != napi_ok)
      NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    else
      NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  nurkel_unregister_tx(ntx);
  nurkel_close_try_close(env, ntree);

  if (worker->destroy || ntx->should_cleanup)
    free(ntx);
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

  status = nurkel_tx_close_work(&params);
  JS_ASSERT(status == napi_ok, JS_ERR_TX_CLOSE_FAIL);

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

  urkel_tx_root(ntx->tx, worker->hash);
  worker->success = true;
}

NURKEL_COMPLETE(tx_root_hash) {
  napi_value result;
  nurkel_tx_t *ntx = NULL;
  nurkel_tx_root_hash_worker_t *worker = data;
  CHECK(worker != NULL);

  ntx = worker->ctx;
  ntx->workers--;

  if (!worker->success || status != napi_ok) {
    NAPI_OK(napi_get_undefined(env, &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    free(worker);
    return;
  }


  NAPI_OK(napi_create_buffer_copy(env,
                                  URKEL_HASH_SIZE,
                                  worker->hash,
                                  NULL,
                                  &result));
  NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  free(worker);
}

NURKEL_METHOD(tx_root_hash) {
  napi_value result, workname;
  napi_status status;
  nurkel_tx_root_hash_worker_t *worker;
  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  JS_ASSERT(napi_create_string_latin1(env,
                                      ASYNC_TX_ROOT_HASH,
                                      NAPI_AUTO_LENGTH,
                                      &workname) == napi_ok, JS_ERR_NODE);

  worker = malloc(sizeof(nurkel_tx_root_hash_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);
  worker->ctx = ntx;

  status = napi_create_promise(env, &worker->deferred, &result);
  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_create_async_work(env,
                                  NULL,
                                  workname,
                                  NURKEL_EXEC_NAME(tx_root_hash),
                                  NURKEL_COMPLETE_NAME(tx_root_hash),
                                  worker,
                                  &worker->work);

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

/*
 * Module
 */

#ifndef NAPI_MODULE_INIT
#define NAPI_MODULE_INIT()                                        \
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
    F(init),
    F(open),
    F(close),
    F(root_hash),
    F(root_hash_sync),
    F(tx_init),
    F(tx_open),
    F(tx_close),
    F(tx_root_hash),
    F(tx_root_hash_sync)
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
