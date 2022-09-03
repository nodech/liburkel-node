/**
 * util.h - utils and helpers for the nurkel.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#ifndef _NURKEL_UTIL_H
#define _NURKEL_UTIL_H

#include <node_api.h>

/*
 * General NAPI Macros
 */

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

#define JS_ASSERT_GOTO_THROW(cond, msg) do { \
  if (!(cond)) {                             \
    err = msg;                               \
    goto throw;                              \
  }                                          \
} while(0)

#define JS_ASSERT(cond, msg) if (!(cond)) JS_THROW(msg)

/*
 * NAPI related macros.
 */

#define NURKEL_METHOD(name)                             \
napi_value                                              \
nurkel_ ## name (napi_env env, napi_callback_info info)

/* EXEC refers to the code that will be executed in the workers. */
#define NURKEL_EXEC_NAME(name) nurkel_ ## name ## _exec

#define NURKEL_EXEC(name)                          \
static void                                        \
nurkel_ ## name ## _exec(napi_env env, void *data)

/* Complete refers to callbacks that are called from the worker thread pool. */
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

#define JS_NAPI_OK(status, msg) JS_ASSERT(status == napi_ok, msg)

#define NURKEL_JS_HASH_OK(arg, var) do { \
  NURKEL_JS_HASH(arg, var);              \
  JS_NAPI_OK((status), JS_ERR_ARG);      \
} while(0)

#define NURKEL_JS_HASH(arg, var) do {              \
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
 * Workers
 */

#define WORKER_BASE_PROPS(ctx_t) \
  ctx_t *ctx;             \
  int err_res;            \
  bool success;           \
  napi_deferred deferred; \
  napi_async_work work;   \
  napi_ref ref;

#define WORKER_INIT(worker) do { \
  worker->err_res = 0;           \
  worker->success = false;       \
  worker->ctx = NULL;            \
  worker->deferred = NULL;       \
  worker->work = NULL;           \
  worker->ref = NULL;            \
} while(0)

void
nurkel_assert_fail(const char *file, int line, const char *expr);

napi_status
read_value_string_latin1(napi_env env, napi_value value,
                         char **str, size_t *length);

napi_status
nurkel_create_error(napi_env env, int err_res, char *msg, napi_value *result);

napi_status
nurkel_create_work(napi_env env,
                   char *name,
                   void *worker,
                   napi_async_work *work,
                   napi_async_execute_callback execute,
                   napi_async_complete_callback complete,
                   napi_deferred *deferred,
                   napi_value *result);

napi_status
nurkel_get_buffer_copy(napi_env env, napi_value value, uint8_t *out,
                       size_t *out_len, const size_t expected,
                       bool expect_lte);

#endif /* _NURKEL_UTIL_H */
