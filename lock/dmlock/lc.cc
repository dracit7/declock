#include <chrono>
#include <vector>
#include <unordered_map>

#include "conf.h"

#include "lc.h"
#include "debug.h"
#include "lm.h"
#include "log.h"
#include "cohort.h"
#include "rdma.h"
#include "remote.h"
#include "util.h"

using std::vector;
using std::unordered_map;

using namespace std::chrono;
using namespace std::chrono_literals;

/**
 * Static configurations, confirmed on setup.
 */
size_t lock_num;

unordered_map<uint32_t, char> lock_state[MAX_CLIENT_NUM];
uint32_t acquire_timestamp[MAX_CLIENT_NUM];

/**
 * Utility functions.
 */
int dmlock_setup_lc(int lk_num) {
  lock_num = lk_num;

  if (!LQ_CAPACITY) LQ_CAPACITY = MAX_QUEUE_SIZE;

#ifdef USE_COORDINATOR
  connect_coordinator();
  cn_join(LHID);
#endif

  return 0;
}

int dmlock_setup_cn(int lock_num) {
  get_conf_from_env();

  bitmap core_map = 0, mn_map = 0;
  for (int i = 0; i < CORE_NUM; i++) BITMAP_SET(core_map, i);
  rdma_setup_cn(NUMA_ID, core_map);
  char* buf = rdma_sendbuf(0, 0, NULL);

  cohort_set_grant_cnt(4);
  return dmlock_setup_lc(lock_num);
}

/**
 * Acquire a read/write lock asynchronously.
 * 
 * Returns 1 if the lock is granted, 0 if the request is suspended.
 */
int dmlock_acquire(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS) {
  bool shared = (op == LOCK_SHARED);

#ifdef USE_TIMESTAMP
  acquire_timestamp[tid] = (timestamp_now(tid) + TS_OFF) & TIMESTAMP_BITMASK;
#endif

  LC_DEBUG("acquire %s cohort lock", shared ? "shared" : "exclusive");
  lock_state[tid][lock] = WAIT_COHORT_LOCK;
  auto ret = cohort_acquire(lock, tid, shared, 
                           &lock_state[tid][lock] ASYNC_ARGS);

  LC_DEBUG("result: %d", ret);
  switch (ret) {
    case CHT_GRANTED: 
      ret = remote_rw_lock(lock, tid, op ASYNC_ARGS);
      cohort_after_grant(lock, tid, ret, shared ASYNC_ARGS);
      return (ret != 0);
    case CHT_SHARED: return 1;
    case CHT_SUSPENDED: return 0;
  }
}

/**
 * Release a read/write lock.
 */
void dmlock_release(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS) {
  bool shared = (op == LOCK_SHARED);
  LC_DEBUG("release %s cohort lock", shared ? "shared" : "exclusive");
  cohort_release(lock, tid, shared ASYNC_ARGS);
}

/**
 * Wait until the lock is granted to a core.
 * 
 * Returns 1 if the lock is granted, 0 if the lock is aborted.
 */
int dmlock_wait_for_grant(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS) {
  while (1) {
    if (dmlock_check_grant(lock, tid, op ASYNC_ARGS)) return 1;
    YIELD;
  }
  return 0;
}

/**
 * Check whether the lock is granted to the task.
 * 
 * Returns 1 if the lock is granted, 0 if the lock has not been granted.
 */
int dmlock_check_grant(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS) {
  int res;

  // If we are waiting for the local cohort lock, check the lock
  // granted flag; if for the remote lock, call the corresponding
  // function to check.
  switch (lock_state[tid][lock]) {
    case WAIT_COHORT_LOCK: 
      remote_check_resets(lock, tid ASYNC_ARGS);
      break;
    case GRANTED: return 1;
    case REACQUIRE_REMOTE_LOCK:
      res = remote_rw_lock(lock, tid, op ASYNC_ARGS);
      if (!res) lock_state[tid][lock] = WAIT_REMOTE_LOCK;
      cohort_after_grant(lock, tid, res, op == LOCK_SHARED ASYNC_ARGS);
      if (res) return 1;
      break;
    case WAIT_REMOTE_LOCK: 
      res = remote_wait_for_lock(lock, tid, op ASYNC_ARGS);
      cohort_after_grant(lock, tid, res, op == LOCK_SHARED ASYNC_ARGS);
      if (res) return 1;

      // If the remote lock is reset, reacquire it.
      lock_state[tid][lock] = REACQUIRE_REMOTE_LOCK;
      break;
  }

  return 0;
}
