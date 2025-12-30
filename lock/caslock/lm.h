#ifndef __CASLOCK_LM_H
#define __CASLOCK_LM_H

#include <stdint.h>

#include "util.h"
#include "conf.h"

#if defined(ASYNC_API)
#include "async_adaptor.h"
#endif

/**
 * The structure of a lock object:
 * 
 * 63                     31                       0
 * +-----------------------+-----------------------+
 * |    exclusive state    |     shared state      |
 * +-----------------------+-----------------------+
*/
typedef uint64_t lock_obj_t;

/**
 * APIs to read the fields of the lock object.
*/
#define SHARED_STATE(l) (uint32_t)((l) & 0xffffffff)
#define EXCL_STATE(l) (uint32_t)(((l) >> 32) & 0xffffffff)

/**
 * API to generate a lock object.
*/
#define LOCK_OBJ(excl_state, shared_state) \
  (lock_obj_t)(\
    ((lock_obj_t)(shared_state) & 0xffffffff) | \
    ((lock_obj_t)((excl_state) & 0xffffffff) << 32) \
)

/**
 * Operations using one-sided RDMA FA.
*/
lock_obj_t fa_lock_obj_generic(uint16_t dest, uint64_t raddr, lock_obj_t add_val ASYNC_PARAMS);
lock_obj_t read_lock_obj(uint16_t dest, uint64_t raddr ASYNC_PARAMS);
bool cas_lock_obj(uint16_t dest, uint64_t raddr, lock_obj_t expected, lock_obj_t swap ASYNC_PARAMS);
bool cas_lock_obj(uint16_t dest, uint64_t raddr, lock_obj_t expected, lock_obj_t swap,\
 lock_obj_t& actual ASYNC_PARAMS);

#define FA_SHARED_STATE(dest, raddr)\
  fa_lock_obj_generic(dest, raddr, LOCK_OBJ(0, 1) ASYNC_ARGS)
#define DEC_SHARED_STATE(dest, raddr)\
  fa_lock_obj_generic(dest, raddr, LOCK_OBJ(0xffffffff, 0xffffffff) ASYNC_ARGS)

/**
 * The memory region size on memory node.
*/
#define MN_LOCK_MR_SIZE(lock_num) \
  ((lock_num) * sizeof(lock_obj_t))

int caslock_setup_lm(int lock_num);

#endif