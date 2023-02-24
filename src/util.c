/**
 * util.c - utils and helpers for the nurkel.
 * Copyright (c) 2022, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/nurkel
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <node_api.h>

#include "common.h"
#include "util.h"

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
nurkel_create_error(napi_env env, int err_res, char *msg, napi_value *result) {
  napi_status status;
  napi_value nmsg, ncode;
  bool has_err_res = false;

  if (err_res > 0 && err_res <= urkel_errors_len) {
    has_err_res = true;
    status = napi_create_string_latin1(env,
                                       urkel_errors[err_res],
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
  } else if (!has_err_res) {
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

napi_status
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

napi_status
nurkel_get_buffer_copy(napi_env env,
                       napi_value value,
                       uint8_t *out,
                       size_t *out_len,
                       const size_t expected,
                       bool expect_lte) {
  bool is_buffer;
  napi_status status;
  uint8_t *buffer;
  size_t buffer_len;

  status = napi_is_buffer(env, value, &is_buffer);

  if (status != napi_ok)
    return status;

  if (!is_buffer)
    return napi_invalid_arg;

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

void
nurkel_buffer_finalize(napi_env env, void *data, void *hint) {
  (void)env;
  (void)hint;

  if (!data)
    return;

  free(data);
}

/*
 * Doubly linked list.
 */

struct nurkel_dlist_entry_s {
  void *item;

  struct nurkel_dlist_entry_s *prev;
  struct nurkel_dlist_entry_s *next;
};

struct nurkel_dlist_s {
  size_t len;
  nurkel_dlist_entry_t *head;
};

nurkel_dlist_t *
nurkel_dlist_alloc(void) {
  nurkel_dlist_t *list = malloc(sizeof(nurkel_dlist_t));

  if (list == NULL)
    return NULL;

  list->head = NULL;
  list->len = 0;

  return list;
}

void
nurkel_dlist_free(nurkel_dlist_t *list) {
  CHECK(list->len == 0);
  free(list);
}

size_t
nurkel_dlist_len(nurkel_dlist_t *list) {
  return list->len;
}

void
nurkel_dlist_remove(nurkel_dlist_t *list, nurkel_dlist_entry_t *entry) {
  CHECK(entry != NULL);
  CHECK(list->len > 0);

  list->len--;

  if (list->len == 0)
    list->head = NULL;

  if (entry->prev != NULL) {
    entry->prev->next = entry->next;
  } else {
    list->head = entry->next;
  }

  if (entry->next != NULL)
    entry->next->prev = entry->prev;

  free(entry);
}

void *
nurkel_dlist_get_value(nurkel_dlist_entry_t *entry) {
  CHECK(entry != NULL);
  return entry->item;
}

nurkel_dlist_entry_t *
nurkel_dlist_insert(nurkel_dlist_t *list, void *item) {
  nurkel_dlist_entry_t *entry = malloc(sizeof(nurkel_dlist_entry_t));
  CHECK(entry != NULL);

  entry->next = NULL;
  entry->prev = NULL;
  entry->item = item;

  list->len++;

  if (list->head == NULL) {
    list->head = entry;
    return entry;
  }

  CHECK(list->head->prev == NULL);
  list->head->prev = entry;
  entry->next = list->head;
  list->head = entry;

  return entry;
}

nurkel_dlist_entry_t *
nurkel_dlist_iter(nurkel_dlist_t *list) {
  return list->head;
}

nurkel_dlist_entry_t *
nurkel_dlist_iter_next(nurkel_dlist_entry_t *entry) {
  if (entry == NULL)
    return NULL;

  return entry->next;
}
