#ifndef __CASLOCK_LC_H
#define __CASLOCK_LC_H

#define CASLOCK_SRC

#include <stdint.h>
#include <stddef.h>
#include "conf.h"
#include "util.h"

#if defined(ASYNC_API)
#include "async_adaptor.h"
#endif

/** 
 * Lock operation types.
 */
typedef enum {
  LOCK_SHARED, /* Shared operation */
  LOCK_EXCL,   /* Exclusive operation */
} lock_op;

/**
 * caslock operations
 */
int caslock_acquire(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS);
int caslock_release(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS);
int caslock_wait_for_lock(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS);
int caslock_setup_lc(bitmap mn_map);

#define acquire_lock(lid, op, tid) caslock_acquire(lid, op, tid ASYNC_ARGS)
#define wait_for_lock(lid, op, tid) \
  caslock_wait_for_lock(lid, op, tid ASYNC_ARGS)
#define release_lock(lid, op, tid) caslock_release(lid, op, tid ASYNC_ARGS)

bool is_lock_granted(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS);
int abort_lock(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS);

#endif