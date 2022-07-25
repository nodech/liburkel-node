#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <node_api.h>
#include <urkel.h>

#define JS_ERR_INIT "Failed to initialize."
#define JS_ERR_NOT_IMPL "Not implemented."

/*
 * Helpers
 */

#define CHECK(expr) do {                           \
  if (!(expr))                                     \
    nurkel_assert_fail(__FILE__, __LINE__, #expr); \
} while (0)

#define JS_THROW(msg) do {                              \
  CHECK(napi_throw_error(env, NULL, (msg)) == napi_ok); \
  return NULL;                                          \
} while (0)

#define JS_ASSERT(cond, msg) if (!(cond)) JS_THROW(msg)

void
nurkel_assert_fail(const char *file, int line, const char *expr) {
  fprintf(stderr, "%s:%d: Assertion `%s' failed.\n", file, line, expr);
  fflush(stderr);
  abort();
}

/*
 * NAPI Context wrappers for the urkel.
 */

typedef struct nurkel_db_s {
  urkel_t *tree;
  napi_ref ref;
  bool is_open;
  bool is_opening;
  bool is_closing;
} nurkel_db_t;

static void
nurkel_db_init(nurkel_db_t *db) {
  db->tree = NULL;
  db->ref = NULL;
  db->is_open = 0;
  db->is_opening = 0;
  db->is_closing = 0;
}

/*
 * DB Init
 */

/*
 * Hook for when the environment exits. This hook will be called after
 * already-scheduled napi_async_work items have finished, which gives us
 * the guarantee that no db operations will be in-flight at this time.
 */

static void
nurkel_db_env_cleanup(void *arg) {
  nurkel_db_t *ndb = (nurkel_db_t *)arg;

  if (ndb->is_open) {
    // TODO: Clean up if there are open iterators or transactions.
    urkel_close(ndb->tree);

    ndb->is_open = false;
    ndb->is_opening = false;
    ndb->is_closing = false;
  }
}

/*
 * This is called when db variable goes out of scope and gets GCed. We need to
 * make sure on the JS side to always have DB in scope until it is being
 * used by iterators and batches. So only time we go out of the scope
 * if everything else went out of scope as well. So on GC we can freely
 * clean up.
 */
static void
nurkel_db_destroy(napi_env env, void *data, void *hint) {
  (void)hint;

  if (data) {
    nurkel_db_t *ndb = (nurkel_db_t *)data;

    // It went out of scope. We don't need to clean up.
    napi_remove_env_cleanup_hook(env, nurkel_db_env_cleanup, ndb);
    // TODO: Maybe close if its open?

    // DB Should be cleaned up by urkel_free
    CHECK(ndb->tree == NULL);
    free(ndb);
  }
}

static napi_value
nurkel_init(napi_env env, napi_callback_info info) {
  (void)info;
  napi_status status;
  napi_value result;
  nurkel_db_t *ctx;

  ctx = malloc(sizeof(nurkel_db_t));
  CHECK(ctx != NULL);
  nurkel_db_init(ctx);

  status = napi_add_env_cleanup_hook(env, nurkel_db_env_cleanup, ctx);

  if (status != napi_ok) {
    free(ctx);
    JS_THROW(JS_ERR_INIT);
  }

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

static napi_value
nurkel_open(napi_env env, napi_callback_info info) {
  (void)info;
  JS_THROW(JS_ERR_NOT_IMPL);
}

static napi_value
nurkel_close(napi_env env, napi_callback_info info) {
  (void)info;
  JS_THROW(JS_ERR_NOT_IMPL);
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
