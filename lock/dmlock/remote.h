
#ifndef __DMLOCK_REMOTE_H
#define __DMLOCK_REMOTE_H

#include <cstdint>
#include "common.h"

/**
 * Cohort logic require mutex locking. We need to ensure that threads
 * still consume notifications from other CNs when waiting for mutex
 * locks to avoid deadlocks. Thus, cohort lib must use mutex locks
 * implemented in this module.
 */
typedef struct {
  uint16_t in;
  uint16_t out;
} cohort_mutex;

void acquire_mutex_of_lock(cohort_mutex* mtx, uint32_t lid, 
                          uint32_t cid ASYNC_PARAMS);
void release_mutex(cohort_mutex* mtx);

int remote_rw_lock(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS);
void remote_rw_unlock(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS);
int remote_wait_for_lock(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS);
void* remote_lock_fetch(uint32_t lock, uint32_t tid ASYNC_PARAMS);
int remote_lock_reset(uint32_t lock, uint32_t tid, uint64_t* hdr ASYNC_PARAMS);
int remote_check_resets(uint32_t lock, uint32_t tid ASYNC_PARAMS);

#endif