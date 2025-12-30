#ifndef __DSLR_LC_H
#define __DSLR_LC_H

#define DSLR_SRC

#include <stdint.h>
#include "util.h"
#include "conf.h"

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
 * dslr lock operations
 */
int dslr_acquire(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS);
int dslr_release(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS);
int dslr_wait_for_lock(uint32_t lock_id, uint8_t op, 
  uint32_t requester ASYNC_PARAMS);
int dslr_setup_lc(size_t lk_num, bitmap core_map);

#define acquire_lock(lid, op, tid) dslr_acquire(lid, op, tid ASYNC_ARGS)
#define wait_for_lock(lid, op, tid) \
  dslr_wait_for_lock(lid, op, tid ASYNC_ARGS)
#define release_lock(lid, op, tid) dslr_release(lid, op, tid ASYNC_ARGS)

bool is_lock_granted(uint32_t lock_id, uint8_t op, uint32_t requester);
int abort_lock(uint32_t lock_id, uint8_t op, uint32_t requester);

#endif