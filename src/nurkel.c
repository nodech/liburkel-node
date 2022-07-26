#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#include <urkel.h>
#include <node_api.h>

#define JS_ERR_INIT "Failed to initialize."
#define JS_ERR_NOT_IMPL "Not implemented."
#define JS_ERR_ARG "Invalid argument."
#define JS_ERR_DB_OPEN "Database is already open."
#define JS_ERR_DB_CLOSED "Database is closed."
#define JS_ERR_ALLOC "Allocation failed."
#define JS_ERR_NODE "Node internal error."

#define JS_ERR_URKEL_UNKNOWN "Unknown urkel error."
#define JS_ERR_URKEL_OPEN "Urkel open failed."
#define JS_ERR_URKEL_CLOSE "Urkel close failed."

#define ASYNC_OPEN "nurkel_open"
#define ASYNC_CLOSE "nurkel_close"

#define CHECK(expr) do {                           \
  if (!(expr))                                     \
    nurkel_assert_fail(__FILE__, __LINE__, #expr); \
} while (0)

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
  napi_ref ref;
  bool is_open;
  bool is_opening;
  bool is_closing;
} nurkel_tree_t;

/*
 * Worker setup
 */

typedef struct nurkel_open_worker_s {
  void *ctx;
  char *path;
  size_t path_len;
  int errno;
  bool success;
  napi_deferred deferred;
  napi_async_work work;
} nurkel_open_worker_t;

typedef struct nurkel_close_worker_s {
  void *ctx;
  int errno;
  bool success;
  napi_deferred deferred;
  napi_async_work work;
} nurkel_close_worker_t;

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
nurkel_db_init(nurkel_tree_t *db) {
  db->tree = NULL;
  db->ref = NULL;
  db->is_open = false;
  db->is_opening = false;
  db->is_closing = false;
}

static void
nurkel_db_uninit(nurkel_tree_t *db) {
  free(db);
}

/*
 * NAPI for urkel.
 *
 * Note that functions ending with _exec are all executed in
 * the worker pool thread. They should not access/call JS
 * or napi_env. If it's absolutely necessary check `napi_thread_safe_function`.
 */

/*
 * DB Init
 */

/*
 * This is called when db variable goes out of scope and gets GCed. We need to
 * make sure on the JS side to always have DB in scope until it is being
 * used by iterators and batches. So only time we go out of the scope
 * if everything else went out of scope as well. So on GC we can freely
 * clean up.
 */
static void
nurkel_db_destroy(napi_env env, void *data, void *hint) {
  (void)env;
  (void)hint;

  if (data) {
    nurkel_tree_t *ctx = (nurkel_tree_t *)data;

    /* TODO: close if its open */

    /* DB Should be cleaned up by urkel_free */
    CHECK(ctx->tree == NULL);
    nurkel_db_uninit(ctx);
  }
}

static napi_value
nurkel_init(napi_env env, napi_callback_info info) {
  (void)info;
  napi_status status;
  napi_value result;
  nurkel_tree_t *ctx;

  ctx = malloc(sizeof(nurkel_tree_t));
  CHECK(ctx != NULL);
  nurkel_db_init(ctx);

  status = napi_create_external(env, ctx, nurkel_db_destroy, NULL, &result);

  if (status != napi_ok) {
    free(ctx);
    JS_THROW(JS_ERR_INIT);
  }

  /* This reference makes sure async work is finished before */
  /* clean up gets triggered. */
  status = napi_create_reference(env, result, 0, &ctx->ref);

  if (status != napi_ok) {
    free(ctx);
    JS_THROW(JS_ERR_INIT);
  }

  return result;
}

/*
 * DB Open and related.
 */

static void
nurkel_open_exec(napi_env env, void *data) {
  (void)env;
  nurkel_open_worker_t *worker = (nurkel_open_worker_t *)data;
  nurkel_tree_t *ctx = (nurkel_tree_t *)worker->ctx;

  ctx->tree = urkel_open(worker->path);

  if (ctx->tree == NULL) {
    worker->errno = urkel_errno;
    return;
  }

  worker->success = true;
}

static void
nurkel_open_complete(napi_env env, napi_status status, void *data) {
  nurkel_open_worker_t *worker = (nurkel_open_worker_t *)data;
  nurkel_tree_t *ctx = (nurkel_tree_t *)worker->ctx;
  napi_value result, msg, code;

  if (status != napi_ok || worker->success == false) {
    ctx->is_opening = false;

    CHECK(worker->errno > 0 && worker->errno <= urkel_errors_len);
    CHECK(napi_create_string_latin1(env,
                                    urkel_errors[worker->errno - 1],
                                    NAPI_AUTO_LENGTH,
                                    &code) == napi_ok);
    CHECK(napi_create_string_latin1(env,
                                    JS_ERR_URKEL_OPEN,
                                    NAPI_AUTO_LENGTH,
                                    &msg) == napi_ok);
    CHECK(napi_create_error(env, code, msg, &result) == napi_ok);
    CHECK(napi_reject_deferred(env, worker->deferred, result) == napi_ok);
    CHECK(napi_delete_async_work(env, worker->work) == napi_ok);
    free(worker->path);
    free(worker);
    return;
  }

  ctx->is_open = true;
  ctx->is_opening = false;
  CHECK(napi_get_undefined(env, &result) == napi_ok);
  CHECK(napi_resolve_deferred(env, worker->deferred, result) == napi_ok);
  CHECK(napi_delete_async_work(env, worker->work) == napi_ok);
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
  nurkel_tree_t *ctx = NULL;
  char *err;

  status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(argc >= 2, JS_ERR_ARG);

  status = napi_get_value_external(env, argv[0], (void **)&ctx);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);

  status = napi_create_string_latin1(env,
                                     ASYNC_OPEN,
                                     NAPI_AUTO_LENGTH,
                                     &workname);
  JS_ASSERT(status == napi_ok, JS_ERR_NODE);
  JS_ASSERT(!ctx->is_open && !ctx->is_opening, JS_ERR_DB_OPEN);
  JS_ASSERT(!ctx->is_closing, JS_ERR_DB_CLOSED);

  worker = malloc(sizeof(nurkel_open_worker_t));
  if (worker == NULL) {
    err = JS_ERR_ALLOC;
    goto throw;
  }

  worker->ctx = ctx;
  worker->path = NULL;
  worker->path_len = 0;
  worker->errno = 0;
  worker->success = false;

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

  ctx->is_opening = true;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    ctx->is_opening = false;
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
 * DB Close
 */

/*
 * This only needs to be called when everything related
 * to database has been cleaned up, because this will
 * free the database.
 */
static void
nurkel_close_exec(napi_env env, void *data) {
  (void)env;
  nurkel_close_worker_t *worker = (nurkel_close_worker_t *)data;
  nurkel_tree_t *ctx = worker->ctx;

  urkel_close(ctx->tree);
  ctx->tree = NULL;
  worker->success = true;
}

/**
 * Same as above, this is called after _exec.
 */
static void
nurkel_close_complete(napi_env env, napi_status status, void *data) {
  nurkel_close_worker_t *worker = (nurkel_close_worker_t *)data;
  nurkel_tree_t *ctx = worker->ctx;
  napi_value result;

  ctx->is_closing = false;
  ctx->is_open = false;

  CHECK(napi_get_undefined(env, &result) == napi_ok);
  CHECK(napi_delete_async_work(env, worker->work) == napi_ok);

  if (status != napi_ok) {
    CHECK(napi_reject_deferred(env, worker->deferred, result) == napi_ok);
    free(worker);
    return;
  }

  CHECK(napi_resolve_deferred(env, worker->deferred, result) == napi_ok);
  free(worker);
}

/**
 * NAPI Call for closing db.
 */
static napi_value
nurkel_close(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_value result, workname;
  napi_status status;
  nurkel_close_worker_t *worker;
  nurkel_tree_t *ctx = NULL;

  status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);
  JS_ASSERT(argc >= 1, JS_ERR_ARG);

  status = napi_get_value_external(env, argv[0], (void **)&ctx);
  JS_ASSERT(status == napi_ok, JS_ERR_ARG);

  status = napi_create_string_latin1(env,
                                     ASYNC_OPEN,
                                     NAPI_AUTO_LENGTH,
                                     &workname);
  JS_ASSERT(status == napi_ok, JS_ERR_NODE);
  JS_ASSERT(ctx->is_open && !ctx->is_closing, JS_ERR_DB_CLOSED);

  worker = malloc(sizeof(nurkel_close_worker_t));
  JS_ASSERT(worker != NULL, JS_ERR_ALLOC);

  worker->ctx = ctx;
  worker->errno = 0;
  worker->success = false;

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

  /* TODO: Check if we have running iterator/batch jobs before
   * running this. If we have, don't run instead store in db->close_worker
   * and set ->is_closing and wait for batch/iterators to call it once
   * no pending job is left.
   */
  ctx->is_closing = true;
  status = napi_queue_async_work(env, worker->work);

  if (status != napi_ok) {
    CHECK(napi_delete_async_work(env, worker->work) == napi_ok);
    free(worker);
  }

  return result;
}

/*
 * Module
 */

#ifndef NAPI_MODULE_INIT
#define NAPI_MODULE_INIT()                                        \
static napi_value bcrypto_init(napi_env env, napi_value exports); \
NAPI_MODULE(NODE_GYP_MODULE_NAME, bcrypto_init)                   \
static napi_value bcrypto_init(napi_env env, napi_value exports)
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
    F(close)
#undef F
  };

  for (i = 0; i < sizeof(funcs) / sizeof(funcs[0]); i++) {
    const char *name = funcs[i].name;
    napi_callback callback = funcs[i].callback;
    napi_value fn;

    CHECK(napi_create_function(env,
                               name,
                               NAPI_AUTO_LENGTH,
                               callback,
                               NULL,
                               &fn) == napi_ok);

    CHECK(napi_set_named_property(env, exports, name, fn) == napi_ok);
  }

  return 0;
}
