#ifndef _NURKEL_BLAKE2B_H
#define _NURKEL_BLAKE2B_H

#include <stddef.h>
#include <stdint.h>
#include <node_api.h>
#include "util.h"

/*
 * BLAKE2b
 */

NURKEL_METHOD(blake2b_create);
NURKEL_METHOD(blake2b_init);
NURKEL_METHOD(blake2b_update);
NURKEL_METHOD(blake2b_final);

#endif /* NURKEL_BLAKE2B_H */
