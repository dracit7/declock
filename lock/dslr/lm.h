#ifndef __DSLR_LM_H
#define __DSLR_LM_H

#include <stdint.h>

#include "util.h"
#include "conf.h"

#if defined(ASYNC_API)
#include "async_adaptor.h"
#endif

/**
 * The lm module.
 * 
 * This module defines the structure of lock object on 
 * memory node used by DSLR. It also provides the APIs to manipulate
 * the lock object stored on memory node.
 */

/**
 * Definition of the lock object.
 * 
 * The structure of a lock object:
 * 
 * 63           47         31           15         0
 * +------------+----------+------------+----------+
 * | shared_max | excl_max | shared_cnt | excl_cnt |
 * +------------+----------+------------+----------+
*/
typedef uint64_t lock_obj_t;

/**
 * APIs to read the fields of the lock object.
*/
#define EXCL_CNT(l) (uint16_t)((l) & 0xffff)
#define SHARED_CNT(l) (uint16_t)(((l) >> 16) & 0xffff)
#define EXCL_MAX(l) (uint16_t)(((l) >> 32) & 0xffff)
#define SHARED_MAX(l) (uint16_t)(((l) >> 48) & 0xffff)

/**
 * API to generate the value of type lock_obj_t.
*/
#define LOCK_OBJ(excl_cnt, shared_cnt, excl_max, shared_max) \
  (lock_obj_t)(\
    ((lock_obj_t)(excl_cnt) & 0xffff) | \
    ((lock_obj_t)((shared_cnt) & 0xffff) << 16) | \
    ((lock_obj_t)((excl_max) & 0xffff) << 32) | \
    ((lock_obj_t)((shared_max) & 0xffff) << 48) \
)

/**
 * Operations using one-sided RDMA FA.
*/
lock_obj_t fa_lock_obj_generic(uint16_t dest, uint64_t raddr, lock_obj_t add_val ASYNC_PARAMS);
lock_obj_t read_lock_obj(uint16_t dest, uint64_t raddr ASYNC_PARAMS);
bool cas_lock_obj(uint16_t dest, uint64_t raddr, lock_obj_t expected, lock_obj_t swap ASYNC_PARAMS);

#define FA_EXCL_CNT(dest, raddr)\
  fa_lock_obj_generic(dest, raddr, LOCK_OBJ(1, 0, 0, 0) ASYNC_ARGS)
#define FA_SHARED_CNT(dest, raddr)\
  fa_lock_obj_generic(dest, raddr, LOCK_OBJ(0, 1, 0, 0) ASYNC_ARGS)
#define FA_EXCL_MAX(dest, raddr)\
  fa_lock_obj_generic(dest, raddr, LOCK_OBJ(0, 0, 1, 0) ASYNC_ARGS)
#define FA_SHARED_MAX(dest, raddr)\
  fa_lock_obj_generic(dest, raddr, LOCK_OBJ(0, 0, 0, 1) ASYNC_ARGS)
#define DEC_EXCL_MAX(dest, raddr)\
  fa_lock_obj_generic(dest, raddr, LOCK_OBJ(0, 0, 0xffff, 0xffff) ASYNC_ARGS)
#define DEC_SHARED_MAX(dest, raddr)\
  fa_lock_obj_generic(dest, raddr, LOCK_OBJ(0, 0, 0, 0xffff) ASYNC_ARGS)

/**
 * The memory region size on memory node.
*/
#define MN_LOCK_MR_SIZE(lock_num) \
  ((lock_num) * sizeof(lock_obj_t))

int dslr_setup_lm(int lock_num);

#endif