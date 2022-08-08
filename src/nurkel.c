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
#define URKEL_VALUE_SIZE 1023
#define URKEL_PROOF_SIZE 17957

#define ERR_UNKNOWN "ERR_UNKNOWN"

#define JS_ERR_INIT "Failed to initialize."
#define JS_ERR_NOT_IMPL "Not implemented."
#define JS_ERR_ARG "Invalid argument."
#define JS_ERR_ALLOC "Allocation failed."
#define JS_ERR_NODE "Node internal error."
#define JS_ERR_UNKNOWN "Unknown internal error."

#define JS_ERR_URKEL_DESTROY "Urkel destroy failed."

#define CHECK(expr) do {                           \
  if (!(expr))                                     \
    nurkel_assert_fail(__FILE__, __LINE__, #expr); \
} while (0)

#define NAPI_OK(expr) do {                         \
  if ((expr) != napi_ok)                           \
    nurkel_assert_fail(__FILE__, __LINE__, #expr); \
} while(0)

#define JS_THROW(msg) do {                               \
  CHECK(napi_throw_error(env, (msg), (msg)) == napi_ok); \
  return NULL;                                           \
} while (0)

#define JS_THROW_CODE(code, msg) do {                     \
  CHECK(napi_throw_error(env, (code), (msg)) == napi_ok); \
  return NULL;                                            \
} while(0)

#define JS_ASSERT(cond, msg) if (!(cond)) JS_THROW(msg)
#define JS_NAPI_OK(status, msg) JS_ASSERT(status == napi_ok, msg)

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

static const int
urkel_errors_len = sizeof(urkel_errors) / sizeof(urkel_errors[0]);

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
  JS_ASSERT(nurkel_tree_ready(ntree), "Tree is not ready.")

#define NURKEL_TX_READY()                                       \
  JS_ASSERT(nurkel_tx_ready(ntx), "Transaction is not ready."); \
  JS_ASSERT(nurkel_tree_ready(ntree), "Tree is not ready.")

/* This needs to be before we have anything to free */
#define NURKEL_JS_WORKNAME(name)                                          \
  JS_ASSERT(napi_create_string_latin1(env,                                \
                                      name,                               \
                                      NAPI_AUTO_LENGTH,                   \
                                      &workname) == napi_ok, JS_ERR_NODE)

#define NURKEL_JS_KEY_OK(arg, var) do {      \
  NURKEL_JS_KEY(arg, var);                   \
  JS_NAPI_OK((status), JS_ERR_ARG); \
} while(0)

#define NURKEL_JS_KEY(arg, var) do {               \
  status = nurkel_get_buffer_copy(env,             \
                                  arg,             \
                                  var,             \
                                  NULL,            \
                                  URKEL_HASH_SIZE, \
                                  false);          \
} while(0)

#define NURKEL_CREATE_ASYNC_WORK(name, worker, result) do { \
  status = nurkel_create_work(env,                                      \
                              "nurkel_" #name,                          \
                              worker,                                   \
                              &worker->work,                            \
                              NURKEL_EXEC_NAME(name),                   \
                              NURKEL_COMPLETE_NAME(name),               \
                              &worker->deferred,                        \
                              &result);                                 \
} while(0)

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
  /* TODO: is_modifying? if we are inserting/removing
   * and commit is called do we guard it? */
} nurkel_tx_t;

/*
 * Worker setup
 */

#define WORKER_BASE_PROPS(ctx_t) \
  ctx_t *ctx;             \
  int errno;              \
  bool success;           \
  napi_deferred deferred; \
  napi_async_work work;   \
  napi_ref ref;

#define WORKER_INIT(worker) do { \
  worker->errno = 0;             \
  worker->success = false;       \
  worker->ctx = NULL;            \
  worker->deferred = NULL;       \
  worker->work = NULL;           \
  worker->ref = NULL;            \
} while(0)

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
} nurkel_get_worker_t;

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
  WORKER_BASE_PROPS(nurkel_tree_t)
  uint8_t in_root[URKEL_HASH_SIZE];
  uint8_t in_key[URKEL_HASH_SIZE];
  uint8_t *in_proof;
  size_t in_proof_len;

  int out_exists;
  uint8_t out_value[URKEL_VALUE_SIZE];
  size_t out_value_len;
} nurkel_verify_worker_t;

/* Transaction workers */

typedef struct nurkel_tx_open_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
} nurkel_tx_open_worker_t;

typedef struct nurkel_tx_close_worker_s {
  WORKER_BASE_PROPS(nurkel_tx_t)
  bool in_destroy;
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
} nurkel_tx_commit_worker_t;

#undef WORKER_BASE_PROPS

/*
 * Close params
 */

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

napi_status
nurkel_create_error(napi_env env, int errno, char *msg, napi_value *result) {
  napi_status status;
  napi_value nmsg, ncode;
  bool has_errno = false;

  if (errno > 0 && errno <= urkel_errors_len) {
    has_errno = true;
    status = napi_create_string_latin1(env,
                                       urkel_errors[errno - 1],
                                       NAPI_AUTO_LENGTH,
                                       &ncode);
  } else {
    status = napi_create_string_latin1(env,
                                       ERR_UNKNOWN,
                                       NAPI_AUTO_LENGTH,
                                       &ncode);
  }

  if (status != napi_ok)
    return status;

  if (msg != NULL) {
    status = napi_create_string_latin1(env, msg, NAPI_AUTO_LENGTH, &nmsg);
  } else if (!has_errno) {
    status = napi_create_string_latin1(env,
                                       JS_ERR_UNKNOWN,
                                       NAPI_AUTO_LENGTH,
                                       &nmsg);
  } else {
    nmsg = ncode;
    ncode = NULL;
  }

  if (status != napi_ok)
    return status;

  return napi_create_error(env, ncode, nmsg, result);
}

static napi_status
nurkel_create_work(napi_env env,
                   char *name,
                   void *worker,
                   napi_async_work *work,
                   napi_async_execute_callback execute,
                   napi_async_complete_callback complete,
                   napi_deferred *deferred,
                   napi_value *result) {
  napi_status status;
  napi_value workname;

  status = napi_create_string_latin1(env, name, NAPI_AUTO_LENGTH, &workname);

  if (status != napi_ok)
    return status;

  status = napi_create_promise(env, deferred, result);

  if (status != napi_ok)
    return status;

  status = napi_create_async_work(env,
                                  NULL,
                                  workname,
                                  execute,
                                  complete,
                                  worker,
                                  work);

  if (status != napi_ok)
    return status;

  return napi_ok;
}

static napi_status
nurkel_get_buffer_copy(napi_env env, napi_value value, uint8_t *out,
                       size_t *out_len, const size_t expected,
                       bool expect_lte) {
  bool is_buffer;
  napi_status status;
  uint8_t *buffer;
  size_t buffer_len;

  status = napi_is_buffer(env, value, &is_buffer);

  if (status != napi_ok)
    return status;

  if (!is_buffer)
    return napi_invalid_arg;;

  status = napi_get_buffer_info(env, value, (void **)&buffer, &buffer_len);

  if (status != napi_ok)
    return status;

  if (out_len != NULL)
    *out_len = buffer_len;

  if (expected != 0 && !expect_lte && buffer_len != expected)
    return napi_invalid_arg;

  if (expected != 0 && expect_lte && buffer_len > expected)
    return napi_invalid_arg;

  memcpy(out, buffer, buffer_len);

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
 *
 * Tree related.
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
    worker->in_destroy = true;
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

  ntree->tree = urkel_open(worker->in_path);

  if (ntree->tree == NULL) {
    worker->errno = urkel_errno;
    return;
  }

  urkel_root(ntree->tree, worker->out_hash);
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
  napi_value result;

  ntree->workers--;

  if (status != napi_ok || worker->success == false) {
    ntree->is_opening = false;

    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
                                "Urkel open failed.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    ntree->is_open = true;
    ntree->is_opening = false;
    memcpy(ntree->root, worker->out_hash, URKEL_HASH_SIZE);

    NAPI_OK(napi_create_buffer_copy(env,
                                    URKEL_HASH_SIZE,
                                    ntree->root,
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

NURKEL_METHOD(open) {
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

  NURKEL_CREATE_ASYNC_WORK(open, worker, result);

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
  worker->in_destroy = params->destroy;

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

  if (worker->deferred != NULL) {
    if (status != napi_ok || worker->success == false) {
      NAPI_OK(nurkel_create_error(env,
                                  worker->errno,
                                  "Failed to close.",
                                  &result));
      NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
    } else {
      NAPI_OK(napi_get_undefined(env, &result));
      NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
    }
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));

  if (worker->in_destroy || ntree->should_cleanup)
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
  urkel_root(ntree->tree, worker->out_hash);
  worker->success = true;
}

NURKEL_COMPLETE(root_hash) {
  napi_value result;

  nurkel_root_hash_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->workers--;

  if (!worker->success || status != napi_ok) {
    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
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

NURKEL_METHOD(root_hash) {
  napi_value result;
  napi_status status;
  nurkel_root_hash_worker_t * worker = NULL;

  NURKEL_ARGV(1);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();

  worker = malloc(sizeof(nurkel_root_hash_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  worker->ctx = ntree;

  status = napi_create_promise(env, &worker->deferred, &result);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  NURKEL_CREATE_ASYNC_WORK(root_hash, worker, result);

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

NURKEL_METHOD(destroy_sync) {
  napi_value result;
  char *path = NULL;
  size_t path_len = 0;

  NURKEL_ARGV(1);

  JS_NAPI_OK(read_value_string_latin1(env, argv[0], &path, &path_len),
             JS_ERR_ARG);

  if (!urkel_destroy(path))
    JS_THROW_CODE(urkel_errors[urkel_errno - 1], JS_ERR_URKEL_DESTROY);

  JS_NAPI_OK(napi_get_undefined(env, &result), JS_ERR_NODE);
  return result;
}

NURKEL_EXEC(destroy) {
  nurkel_destroy_worker_t *worker = data;

  if (!urkel_destroy(worker->in_path)) {
    worker->errno = urkel_errno;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(destroy) {
  napi_value result;
  nurkel_destroy_worker_t *worker = data;

  if (status != napi_ok || worker->errno != 0) {
    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
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

  NURKEL_ARGV(1);

  worker = malloc(sizeof(nurkel_destroy_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_NODE);
  WORKER_INIT(worker);
  worker->in_path = NULL;
  worker->in_path_len = 0;

  JS_NAPI_OK(read_value_string_latin1(env,
                                      argv[0],
                                      &worker->in_path,
                                      &worker->in_path_len), JS_ERR_NODE);

  NURKEL_CREATE_ASYNC_WORK(destroy, worker, result);

  if (status != napi_ok) {
    free(worker->in_path);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    free(worker->in_path);
    free(worker);
    JS_THROW(JS_ERR_NODE);
  }

  return result;
}

NURKEL_METHOD(hash_sync) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

/* NURKEL_EXEC(hash) { */
/* } */

/* NURKEL_COMPLETE(hash) { */
/* } */

NURKEL_METHOD(hash) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

NURKEL_METHOD(inject_sync) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

/* NURKEL_EXEC(inject) { */
/* } */

/* NURKEL_COMPLETE(inject) { */
/* } */

NURKEL_METHOD(inject) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

NURKEL_METHOD(get_sync) {
  napi_value result;
  napi_status status;
  uint8_t key[URKEL_HASH_SIZE];
  uint8_t value[URKEL_VALUE_SIZE];
  size_t value_len;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();
  NURKEL_JS_KEY_OK(argv[1], key);

  if (!urkel_get(ntree->tree, value, &value_len, key, NULL))
    JS_THROW_CODE(urkel_errors[urkel_errno - 1], "Failed to get.");

  JS_NAPI_OK(napi_create_buffer_copy(env,
                                     value_len,
                                     value,
                                     NULL,
                                     &result), JS_ERR_NODE);

  return result;
}

NURKEL_EXEC(get) {
  (void)env;

  nurkel_get_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  if (!urkel_get(ntree->tree,
                 worker->out_value,
                 &worker->out_value_len,
                 worker->in_key,
                 NULL)) {
    worker->errno = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(get) {
  napi_value result;
  nurkel_get_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
                                "Failed to get.",
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
  NAPI_OK(nurkel_close_try_close(env, ntree));
  free(worker);
}

NURKEL_METHOD(get) {
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

  NURKEL_JS_KEY(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(get, worker, result);

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

NURKEL_METHOD(has_sync) {
  napi_value result;
  napi_status status;
  uint8_t key[URKEL_HASH_SIZE];
  bool has_key = true;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();
  NURKEL_JS_KEY_OK(argv[1], key);

  if (!urkel_has(ntree->tree, key, NULL)) {
    if (urkel_errno != URKEL_ENOTFOUND)
      JS_THROW(urkel_errors[urkel_errno - 1]);

    has_key = false;
  }

  JS_NAPI_OK(napi_get_boolean(env, has_key, &result), JS_ERR_NODE);
  return result;
}

NURKEL_EXEC(has) {
  (void)env;

  nurkel_has_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  if (!urkel_has(ntree->tree, worker->in_key, NULL)) {
    if (urkel_errno != URKEL_ENOTFOUND) {
      worker->errno = urkel_errno;
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

NURKEL_COMPLETE(has) {
  napi_value result;
  nurkel_has_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;
  ntree->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
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

NURKEL_METHOD(has) {
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

  NURKEL_JS_KEY(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(has, worker, result);

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

NURKEL_METHOD(insert_sync) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

/* NURKEL_EXEC(insert) { */
/* } */

/* NURKEL_COMPLETE(insert) { */
/* } */

NURKEL_METHOD(insert) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

NURKEL_METHOD(remove_sync) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

/* NURKEL_EXEC(remove) { */
/* } */

/* NURKEL_COMPLETE(remove) { */
/* } */

NURKEL_METHOD(remove) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

NURKEL_METHOD(prove_sync) {
  napi_value result;
  napi_status status;
  uint8_t key_buffer[URKEL_HASH_SIZE];
  uint8_t *proof_raw = NULL;
  size_t proof_len;

  NURKEL_ARGV(2);
  NURKEL_TREE_CONTEXT();
  NURKEL_TREE_READY();
  NURKEL_JS_KEY_OK(argv[1], key_buffer);

  if (!urkel_prove(ntree->tree, &proof_raw, &proof_len, key_buffer, NULL))
    JS_THROW(urkel_errors[urkel_errno - 1]);

  status = napi_create_buffer_copy(env,
                                   proof_len,
                                   proof_raw,
                                   NULL,
                                   &result);
  free(proof_raw);

  if (status != napi_ok)
    JS_THROW(JS_ERR_NODE);

  return result;
}

NURKEL_EXEC(prove) {
  (void)env;

  nurkel_prove_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  if (!urkel_prove(ntree->tree, &worker->out_proof, &worker->out_proof_len,
                      worker->in_key, NULL)) {
    worker->errno = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(prove) {
  napi_value result;
  nurkel_prove_worker_t *worker = data;
  nurkel_tree_t *ntree = worker->ctx;

  ntree->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
                                "Failed to prove.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    CHECK(worker->out_proof != NULL);
    NAPI_OK(napi_create_buffer_copy(env,
                                    worker->out_proof_len,
                                    worker->out_proof,
                                    NULL,
                                    &result));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  NAPI_OK(napi_delete_async_work(env, worker->work));
  NAPI_OK(nurkel_close_try_close(env, ntree));
  if (worker->out_proof != NULL)
    free(worker->out_proof);
  free(worker);
}

NURKEL_METHOD(prove) {
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

  NURKEL_JS_KEY(argv[1], worker->in_key);

  if (status != napi_ok) {
    free(worker);
    JS_THROW(JS_ERR_ARG);
  }

  NURKEL_CREATE_ASYNC_WORK(prove, worker, result);

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
  napi_value result_exists;
  napi_value result;
  napi_status status;
  uint8_t root[URKEL_HASH_SIZE];
  uint8_t key[URKEL_HASH_SIZE];
  uint8_t *proof;
  uint8_t value[URKEL_VALUE_SIZE];
  size_t value_len = 0;
  size_t proof_len;

  NURKEL_ARGV(3);
  NURKEL_JS_KEY_OK(argv[1], key);
  NURKEL_JS_KEY_OK(argv[2], root);

  JS_NAPI_OK(napi_get_buffer_info(env, argv[0], NULL, &proof_len), JS_ERR_ARG);
  proof = malloc(proof_len);
  JS_ASSERT(proof != NULL, JS_ERR_ALLOC);
  JS_ASSERT(proof_len <= URKEL_PROOF_SIZE, JS_ERR_ARG);

  JS_NAPI_OK(nurkel_get_buffer_copy(env,
                                    argv[0],
                                    proof,
                                    &proof_len,
                                    proof_len,
                                    false), JS_ERR_ARG);

  int exists = 0;

  if (!urkel_verify(&exists, value, &value_len, proof, proof_len, key, root)) {
    free(proof);
    JS_THROW_CODE(urkel_errors[urkel_errno - 1], "Failed to verify_sync.");
  }

  free(proof);

  JS_NAPI_OK(napi_create_array_with_length(env, 2, &result), JS_ERR_NODE);
  JS_NAPI_OK(napi_get_boolean(env, exists, &result_exists), JS_ERR_NODE);
  JS_NAPI_OK(napi_create_buffer_copy(env,
                                     value_len,
                                     value,
                                     NULL,
                                     &result_value), JS_ERR_NODE);

  JS_NAPI_OK(napi_set_element(env, result, 0, result_exists), JS_ERR_NODE);
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
    worker->errno = urkel_errno;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(verify) {
  napi_value result;
  napi_value result_exists;
  napi_value result_value;
  nurkel_verify_worker_t *worker = data;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
                                "Failed to verify.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    NAPI_OK(napi_create_array_with_length(env, 2, &result));
    NAPI_OK(napi_get_boolean(env, worker->out_exists, &result_exists));
    NAPI_OK(napi_create_buffer_copy(env,
                                    worker->out_value_len,
                                    worker->out_value,
                                    NULL,
                                    &result_value));

    NAPI_OK(napi_set_element(env, result, 0, result_exists));
    NAPI_OK(napi_set_element(env, result, 1, result_value));
    NAPI_OK(napi_resolve_deferred(env, worker->deferred, result));
  }

  free(worker->in_proof);
  free(worker);
}

NURKEL_METHOD(verify) {
  napi_value result;
  napi_status status;
  nurkel_verify_worker_t *worker;
  size_t proof_len;
  char *err;

  NURKEL_ARGV(3);

  JS_NAPI_OK(napi_get_buffer_info(env, argv[0], NULL, &proof_len), JS_ERR_ARG);

  worker = malloc(sizeof(nurkel_verify_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);
  WORKER_INIT(worker);

  worker->in_proof = malloc(proof_len);
  if (worker->in_proof == NULL) {
    free(worker);
    JS_THROW(JS_ERR_ALLOC);
  }

  NURKEL_JS_KEY(argv[1], worker->in_key);
  if (status != napi_ok) {
    err = JS_ERR_ARG;
    goto throw;
  }

  NURKEL_JS_KEY(argv[2], worker->in_root);
  if (status != napi_ok) {
    err = JS_ERR_ARG;
    goto throw;
  }

  status = nurkel_get_buffer_copy(env,
                                  argv[0],
                                  worker->in_proof,
                                  &worker->in_proof_len,
                                  proof_len,
                                  false);

  if (status != napi_ok) {
    err = JS_ERR_ARG;
    goto throw;
  }

  NURKEL_CREATE_ASYNC_WORK(verify, worker, result);

  if (status != napi_ok) {
    err = JS_ERR_NODE;
    goto throw;
  }

  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    napi_delete_async_work(env, worker->work);
    err = JS_ERR_NODE;
    goto throw;
  }

  return result;

throw:
  free(worker->in_proof);
  free(worker);
  JS_THROW(err);
}

/*
 * Transaction related.
 */

NURKEL_EXEC_DECL(tx_close);
NURKEL_COMPLETE_DECL(tx_close);

static void
nurkel_tx_destroy(napi_env env, void *data, void *hint) {
  (void)hint;

  CHECK(data != NULL);

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
    worker->in_destroy = true;
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

  tx = urkel_tx_create(ntree->tree, ntx->root);

  if (tx == NULL) {
    worker->success = false;
    worker->errno = urkel_errno;
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

    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
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
    memcpy(ntx->root, buffer, URKEL_HASH_SIZE);
  } else {
    memcpy(ntx->root, &ntree->root, URKEL_HASH_SIZE);
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

static napi_status
nurkel_tx_close_work(nurkel_tx_close_params_t *params) {
  napi_value workname;
  napi_status status;
  nurkel_tx_close_worker_t *worker;
  nurkel_tx_t *ntx = params->ctx;

  if (ntx->is_closing || ntx->should_close)
    return napi_ok;

  status = napi_create_string_latin1(params->env,
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
  worker->in_destroy = params->destroy;

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
    napi_delete_async_work(params->env, worker->work);
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

  if (worker->deferred != NULL && status != napi_ok) {
    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
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

  if (worker->in_destroy || ntx->should_cleanup)
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
                                worker->errno,
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
    worker->errno = urkel_errno;
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
                                worker->errno,
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
  NURKEL_JS_KEY(argv[1], worker->in_key);

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
  NURKEL_JS_KEY_OK(argv[1], key_buffer);

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
      worker->errno = urkel_errno;
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
                                worker->errno,
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

  NURKEL_JS_KEY(argv[1], worker->in_key);

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
  NURKEL_JS_KEY_OK(argv[1], key_buffer);

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
    worker->errno = urkel_errno;
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
                                worker->errno,
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

  NURKEL_JS_KEY(argv[1], worker->in_key);

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
  NURKEL_JS_KEY_OK(argv[1], key_buffer);

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
    worker->errno = urkel_errno;
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
                                worker->errno,
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

  NURKEL_JS_KEY(argv[1], worker->in_key);

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
  uint8_t key_buffer[URKEL_HASH_SIZE];
  uint8_t *proof_raw = NULL;
  size_t proof_len;

  NURKEL_ARGV(2);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();
  NURKEL_JS_KEY_OK(argv[1], key_buffer);

  if (!urkel_tx_prove(ntx->tx, &proof_raw, &proof_len, key_buffer))
    JS_THROW(urkel_errors[urkel_errno - 1]);

  status = napi_create_buffer_copy(env,
                                   proof_len,
                                   proof_raw,
                                   NULL,
                                   &result);

  free(proof_raw);

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
    worker->errno = urkel_errno;
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
                                worker->errno,
                                "Failed to tx prove.",
                                &result));
    NAPI_OK(napi_reject_deferred(env, worker->deferred, result));
  } else {
    CHECK(worker->out_proof != NULL);
    /* TODO: Use create_external_buffer ??. */
    /* TODO: buffer vs arraybuffer. */
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

  NURKEL_JS_KEY(argv[1], worker->in_key);

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

  NURKEL_ARGV(1);
  NURKEL_TX_CONTEXT();
  NURKEL_TX_READY();

  JS_NAPI_OK(napi_get_undefined(env, &result), JS_ERR_NODE);

  if (!urkel_tx_commit(ntx->tx))
    JS_THROW(urkel_errors[urkel_errno - 1]);

  return result;
}

NURKEL_EXEC(tx_commit) {
  (void)env;

  nurkel_tx_commit_worker_t *worker = data;
  nurkel_tx_t *ntx = worker->ctx;

  if (!urkel_tx_commit(ntx->tx)) {
    worker->errno = urkel_errno;
    worker->success = false;
    return;
  }

  worker->success = true;
}

NURKEL_COMPLETE(tx_commit) {
  napi_value result;
  nurkel_tx_commit_worker_t *worker = data;;
  nurkel_tx_t *ntx = worker->ctx;

  ntx->workers--;

  if (status != napi_ok || worker->success == false) {
    NAPI_OK(nurkel_create_error(env,
                                worker->errno,
                                "Failed to tx commit.",
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
  JS_THROW(JS_ERR_NOT_IMPL);
}

/* NURKEL_EXEC(tx_clear) { */
/* } */

/* NURKEL_COMPLETE(tx_clear) { */
/* } */

NURKEL_METHOD(tx_clear) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

NURKEL_METHOD(tx_inject_sync) {
  JS_THROW(JS_ERR_NOT_IMPL);
}

/* NURKEL_EXEC(tx_inject) { */
/* } */

/* NURKEL_COMPLETE(tx_inject) { */
/* } */

NURKEL_METHOD(tx_inject) {
  JS_THROW(JS_ERR_NOT_IMPL);
}


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
    F(init),
    F(open),
    F(close),
    F(root_hash_sync),
    F(root_hash),
    F(destroy_sync),
    F(destroy),
    F(hash_sync),
    F(hash),
    F(inject_sync),
    F(inject),
    F(get_sync),
    F(get),
    F(has_sync),
    F(has),
    F(insert_sync),
    F(insert),
    F(remove_sync),
    F(remove),
    F(prove_sync),
    F(prove),
    F(verify_sync),
    F(verify),

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
    F(tx_inject)
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
