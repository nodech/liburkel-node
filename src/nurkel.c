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
#define ASYNC_TX_OPEN "nurkel_tx_open"
#define ASYNC_TX_CLOSE "nurkel_tx_close"
#define ASYNC_TX_DESTROY "nurkel_tx_destroy"

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
 * NAPI Context wrappers for the urkel.
 */

typedef struct nurkel_tree_s {
  urkel_t *tree;
  unsigned char root[URKEL_HASH_SIZE];
  napi_ref ref;
  uint32_t dependencies;
  void *close_worker;
  bool is_open;
  bool is_opening;
  bool is_closing;
  bool should_close;
} nurkel_tree_t;

typedef struct nurkel_tx_s {
  nurkel_tree_t *ntree;
  urkel_tx_t *tx;
  unsigned char root[URKEL_HASH_SIZE];
  uint32_t processing;
  void *close_worker;
  bool is_open;
  bool is_opening;
  bool is_closing;
  bool should_close;
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
  unsigned char hash[URKEL_HASH_SIZE];
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

typedef struct nurkel_tx_close_params_s {
  napi_env env;
  nurkel_tx_t *ntx;
  napi_value *promise_result;
  bool promise;
  bool destroy;
} nurkel_tx_close_params_t;

#undef WORKER_BASE_PROPS

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
  ntree->dependencies = 0;
  ntree->is_open = false;
  ntree->is_opening = false;
  ntree->is_closing = false;
  ntree->should_close = false;
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
  ntx->is_open = false;
  ntx->is_opening = false;
  ntx->is_closing = false;
  ntx->processing = 0;
  memset(ntx->root, 0, URKEL_HASH_SIZE);
}

static inline bool
nurkel_tx_ready(nurkel_tx_t *ntx) {
  return ntx->is_open && !ntx->is_closing
    && !ntx->is_opening && !ntx->should_close;
}

/*
 * NAPI for urkel.
 *
 * Note that functions ending with _exec are all executed in
 * the worker pool thread. They should not access/call JS
 * or napi_env. If it's absolutely necessary check `napi_thread_safe_function`.
 */

/*
 * Tree Init
 */

static void
nurkel_close_exec(napi_env env, void *data);
static void
nurkel_close_complete(napi_env env, napi_status status, void *data);

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

  /* GC Started before open or close promise finished. We just crash. */
  CHECK(!ntree->is_opening && !ntree->is_closing);

  if (ntree->is_open) {
    /* User forgot to clean up before ref got out of the scope . */
    /* Make sure we do the clean up. */
    napi_value workname;
    NAPI_OK(napi_create_string_latin1(env,
                                      ASYNC_DESTROY,
                                      NAPI_AUTO_LENGTH,
                                      &workname));

    nurkel_close_worker_t *worker = malloc(sizeof(nurkel_close_worker_t));
    CHECK(worker != NULL);

    WORKER_INIT(worker);
    worker->ctx = ntree;
    worker->destroy = true;

    NAPI_OK(napi_create_async_work(env,
                                   NULL,
                                   workname,
                                   nurkel_close_exec,
                                   nurkel_close_complete,
                                   worker,
                                   &worker->work));


    if (ntree->dependencies == 0) {
      ntree->is_closing = true;
      NAPI_OK(napi_queue_async_work(env, worker->work));
      return;
    }

    ntree->should_close = true;
    ntree->close_worker = worker;
    return;
  }

  free(ntree);
}

static napi_value
nurkel_init(napi_env env, napi_callback_info info) {
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

static void
nurkel_open_exec(napi_env env, void *data) {
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

static void
nurkel_open_complete(napi_env env, napi_status status, void *data) {
  nurkel_open_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;
  napi_value result, msg, code;

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
    NAPI_OK(napi_delete_async_work(env, worker->work));
    free(worker->path);
    free(worker);
    return;
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
  NAPI_OK(napi_delete_async_work(env, worker->work));
  free(worker->path);
  free(worker);
}

static napi_value
nurkel_open(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_value result, workname;
  napi_status status;
  nurkel_open_worker_t *worker = NULL;
  nurkel_tree_t *ntree = NULL;
  char *err;

  status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(argc == 2, JS_ERR_ARG);

  status = napi_get_value_external(env, argv[0], (void **)&ntree);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);

  status = napi_create_string_latin1(env,
                                     ASYNC_OPEN,
                                     NAPI_AUTO_LENGTH,
                                     &workname);
  JS_ASSERT(status == napi_ok, JS_ERR_NODE);
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
                                  nurkel_open_exec,
                                  nurkel_open_complete,
                                  worker,
                                  &worker->work);

  if (status != napi_ok) {
    err = JS_ERR_NODE;
    goto throw;
  }

  ntree->is_opening = true;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntree->is_opening = false;
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

/*
 * This only needs to be called when everything related
 * to database has been cleaned up, because this will
 * free the database.
 */
static void
nurkel_close_exec(napi_env env, void *data) {
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

static void
nurkel_close_complete(napi_env env, napi_status status, void *data) {
  nurkel_close_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;
  napi_value result;

  ntree->is_closing = false;
  ntree->is_open = false;

  NAPI_OK(napi_delete_async_work(env, worker->work));

  if (worker->deferred != NULL) {
    NAPI_OK(napi_get_undefined(env, &result));

    if (status != napi_ok)
      NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    else
      NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  if (worker->destroy)
    free(ntree);
  free(worker);
}

/**
 * NAPI Call for closing tree.
 * This will wait indefintely if dependencies are not closed first???
 */
static napi_value
nurkel_close(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_value result, workname;
  napi_status status;
  nurkel_close_worker_t *worker;
  nurkel_tree_t *ntree = NULL;

  status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(argc == 1, JS_ERR_ARG);

  status = napi_get_value_external(env, argv[0], (void **)&ntree);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(nurkel_tree_ready(ntree), JS_ERR_TREE_NOTREADY);

  status = napi_create_string_latin1(env,
                                     ASYNC_OPEN,
                                     NAPI_AUTO_LENGTH,
                                     &workname);
  JS_ASSERT(status == napi_ok, JS_ERR_NODE);

  worker = malloc(sizeof(nurkel_close_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  worker->destroy = false;

  WORKER_INIT(worker);
  worker->ctx = ntree;

  status = napi_create_promise(env, &worker->deferred, &result);
  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_create_async_work(env,
                                  NULL,
                                  workname,
                                  nurkel_close_exec,
                                  nurkel_close_complete,
                                  worker,
                                  &worker->work);
  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }


  if (ntree->dependencies > 0) {
    ntree->should_close = true;
    ntree->close_worker = worker;
    return result;
  }

  ntree->is_closing = true;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }
  return result;
}

/*
 * Transaction related
 */

static void
nurkel_tx_close_exec(napi_env env, void *data);

static void
nurkel_tx_close_complete(napi_env env, napi_status status, void *data);

static void
nurkel_tx_destroy(napi_env env, void *data, void *hint) {
  (void)hint;

  if (!data)
    return;

  nurkel_tx_t *ntx = data;
  nurkel_tree_t *ntree = ntx->ntree;

  CHECK(!ntx->is_opening && !ntx->is_closing);
  NAPI_OK(napi_reference_unref(env, ntree->ref, NULL));

  if (ntx->is_open) {
    napi_value workname;
    NAPI_OK(napi_create_string_latin1(env,
                                      ASYNC_TX_DESTROY,
                                      NAPI_AUTO_LENGTH,
                                      &workname));

    nurkel_tx_close_worker_t *worker = malloc(sizeof(nurkel_tx_open_worker_t));
    CHECK(worker != NULL);
    WORKER_INIT(worker);
    worker->ctx = ntx;
    worker->destroy = true;

    NAPI_OK(napi_create_async_work(env,
                                   NULL,
                                   workname,
                                   nurkel_tx_close_exec,
                                   nurkel_tx_close_complete,
                                   worker,
                                   &worker->work));

    if (ntx->processing == 0) {
      ntx->is_closing = true;
      NAPI_OK(napi_queue_async_work(env, worker->work));
      return;
    }

    ntx->should_close = true;
    ntx->close_worker = worker;
    return;
  }

  if (ntree->should_close && ntree->dependencies == 0) {
    CHECK(ntree->close_worker != NULL);
    ntree->is_closing = true;
    nurkel_close_worker_t *worker = ntree->close_worker;
    NAPI_OK(napi_queue_async_work(env, worker->work));
  }

  free(ntx);
}

static napi_value
nurkel_tx_init(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_value result;
  napi_status status;
  nurkel_tree_t *ntree;
  nurkel_tx_t *ntx;

  status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(argc == 1, JS_ERR_ARG);

  /* Grab the tree */
  status = napi_get_value_external(env, argv[0], (void **)&ntree);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(ntree != NULL, JS_ERR_ARG);
  JS_ASSERT(nurkel_tree_ready(ntree), JS_ERR_TREE_NOTREADY);

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

static void
nurkel_tx_open_exec(napi_env env, void *data) {
  (void)env;

  nurkel_tx_open_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  nurkel_tree_t *ntree = ntx->ntree;

  ntx->tx = urkel_tx_create(ntree->tree, ntx->root);
  worker->success = true;
}

static void
nurkel_tx_open_complete(napi_env env, napi_status status, void *data) {
  nurkel_tx_open_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  /* nurkel_tree_t *ntree = ntx->ntree; */
  napi_value result;

  ntx->is_open = true;
  ntx->is_opening = false;
  NAPI_OK(napi_get_undefined(env, &result));
  NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));

  if (ntx->should_close) {
    /* TODO: start closing this shit. */
  }
  /* At this point, tree could have started closing and waiting for tx ?
   */
  free(worker);
}

static napi_value
nurkel_tx_open(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_value result, workname;
  napi_status status;
  napi_valuetype type;
  bool is_buffer = false;
  unsigned char *buffer = NULL;
  size_t buffer_len;
  nurkel_tree_t *ntree;
  nurkel_tx_t *ntx;
  nurkel_tx_open_worker_t *worker;

  status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(argc == 2, JS_ERR_ARG);

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
                                  nurkel_tx_open_exec,
                                  nurkel_tx_open_complete,
                                  worker,
                                  &worker->work);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  /* Make sure DB does not close and free while we are working with it. */
  ntree->dependencies++;
  ntx->is_opening = true;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ntree->is_opening = false;
    napi_delete_async_work(env, worker->work);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}

static void
nurkel_tx_close_exec(napi_env env, void *data) {
  (void)env;
  nurkel_tx_close_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  urkel_tx_destroy(ntx->tx);
  ntx->tx = NULL;
  worker->success = true;
}

static void
nurkel_tx_close_complete(napi_env env, napi_status status, void *data) {
  nurkel_tx_close_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;
  nurkel_tree_t *ntree = ntx->ntree;
  napi_value result;

  ntx->is_closing = false;
  ntx->is_open = false;

  NAPI_OK(napi_delete_async_work(env, worker->work));

  if (worker->deferred != NULL) {
    NAPI_OK(napi_get_undefined(env, &result));

    if (status != napi_ok)
      NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    else
      NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  ntree->dependencies--;
  if (ntree->dependencies == 0 && ntree->should_close) {
    nurkel_close_worker_t *ntree_worker = ntree->close_worker;
    NAPI_OK(napi_queue_async_work(env, ntree_worker->work));
  }

  if (worker->destroy)
    free(ntx);
  free(worker);
}

static napi_status
nurkel_tx_close_work(nurkel_tx_close_params_t params) {
  napi_value workname;
  napi_status status;
  nurkel_close_worker_t *worker;

  status = napi_create_string_latin1(params.env,
                                     ASYNC_TX_CLOSE,
                                     NAPI_AUTO_LENGTH,
                                     &workname);

  if (status != napi_ok)
    return status;

  worker = malloc(sizeof(nurkel_close_worker_t));
  if (worker == NULL)
    return napi_generic_failure;

  WORKER_INIT(worker);
  worker->ctx = params.ntx;;
  worker->destroy = params.destroy;

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
                                  nurkel_tx_close_exec,
                                  nurkel_tx_close_complete,
                                  worker,
                                  &worker->work);
  if (status != napi_ok) {
    free(worker);
    return status;
  }

  if (params.ntx->processing > 0) {
    params.ntx->should_close = true;
    params.ntx->close_worker = worker;
    return napi_ok;
  }

  params.ntx->is_closing = true;
  status = napi_queue_async_work(params.env, worker->work);

  if (status != napi_ok) {
    free(worker);
    return napi_generic_failure;
  }

  return napi_ok;
}

static napi_value
nurkel_tx_close(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_value result;
  napi_status status;
  nurkel_tx_t *ntx = NULL;
  nurkel_tree_t *ntree = NULL;
  nurkel_tx_close_params_t params = {};

  status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(argc == 1, JS_ERR_ARG);

  status = napi_get_value_external(env, argv[0], (void **)&ntx);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(ntx != NULL, JS_ERR_ARG);
  ntree = ntx->ntree;

  JS_ASSERT(nurkel_tx_ready(ntx), JS_ERR_TX_NOTREADY);
  JS_ASSERT(nurkel_tree_ready(ntree), JS_ERR_TREE_NOTREADY);

  params.env = env;
  params.ntx = ntx;
  params.promise = true;
  params.promise_result = &result;
  params.destroy = false;

  status = nurkel_tx_close_work(params);
  JS_ASSERT(status == napi_ok, JS_ERR_TX_CLOSE_FAIL);

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
    F(tx_init),
    F(tx_open),
    F(tx_close)
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
