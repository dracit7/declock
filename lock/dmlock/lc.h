
#ifndef __DMLOCK_LC_H
#define __DMLOCK_LC_H

#include <stdint.h>
#include "common.h"

#define DMLOCK_SRC

extern "C" {
int dmlock_acquire(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS);
void dmlock_release(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS);
int dmlock_wait_for_grant(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS);
int dmlock_check_grant(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS);

#ifdef USE_COHORT

#define acquire_lock(lid, op, tid) dmlock_acquire(lid, tid, op ASYNC_ARGS)
#define wait_for_lock(lid, op, tid) \
  dmlock_wait_for_grant(lid, tid, op ASYNC_ARGS)
#define release_lock(lid, op, tid) dmlock_release(lid, tid, op ASYNC_ARGS)

#else

#define acquire_lock(lid, op, tid) remote_rw_lock(lid, tid, op ASYNC_ARGS)
#define wait_for_lock(lid, op, tid) \
  remote_wait_for_lock(lid, tid, op ASYNC_ARGS)
#define release_lock(lid, op, tid) remote_rw_unlock(lid, tid, op ASYNC_ARGS)

#endif

int dmlock_setup_lc(int lk_num);
int dmlock_setup_cn(int lock_num);
}

extern size_t lock_num;
extern uint32_t acquire_timestamp[MAX_CLIENT_NUM];

#endif