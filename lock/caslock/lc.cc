
#include "lc.h"
#include "rdma.h"
#include "debug.h"
#include "lm.h"
#include "conf.h"
#include "lcore.h"

/**
 * Util functions to partition lock among
 * memory nodes.
 */

static inline uint64_t lock_obj_raddr(uint32_t lock_id) {
  return (uint64_t)(lock_id * sizeof(lock_obj_t));
}

/**
 * Acquire a shared or exclusive lock asynchronously.
 * 
 * Returns 1 if the lock is granted, 0 if the request is suspended.
 */
static inline int acquire_excl_lock(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  bool acquire_success = 
    cas_lock_obj(lm_of_lock(lock_id), lock_obj_raddr(lock_id), LOCK_OBJ(0, 0), LOCK_OBJ(requester, 0) ASYNC_ARGS);
  if (acquire_success) {
    return 1;
  } else {
    return 0;
  }
}

static inline int acquire_shared_lock(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  lock_obj_t cur_state = FA_SHARED_STATE(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
  if (EXCL_STATE(cur_state) == 0) {
    return 1;
  } else {
    return 0;
  }
}

/**
 * Acquire a read/write lock asynchronously.
 * 
 * Returns 1 if the lock is granted, 0 if the request is suspended.
 */
int caslock_acquire(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS) {
  if (op == LOCK_EXCL) {
    return acquire_excl_lock(lock_id, requester ASYNC_ARGS);
  } else {
    return acquire_shared_lock(lock_id, requester ASYNC_ARGS);
  }
}

/**
 * Release a lock.
 * 
 * Always return 0, becasuse when the functions returns, the lock must be released.
 */
static inline int release_excl_lock(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  uint16_t lm_id = lm_of_lock(lock_id);
  uint64_t lock_addr = lock_obj_raddr(lock_id);
  lock_obj_t actual_state = 0;
  uint32_t shared_state = SHARED_STATE(actual_state);

  // If anyone attempts to acquire the shared lock, 
  // the shared state will be increased by one, 
  // so using the while loop and continuously modify the shared state 
  // to make sure the CAS can be successful.
  while (!cas_lock_obj(lm_id, lock_addr, LOCK_OBJ(requester, shared_state), LOCK_OBJ(0, shared_state), actual_state ASYNC_ARGS)) {
    shared_state = SHARED_STATE(actual_state);
  }
  return 0;
}

static inline int release_shared_lock(uint32_t lock_id, uint32_t requster ASYNC_PARAMS) {
  DEC_SHARED_STATE(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
  return 0;
}

int caslock_release(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS) {
  if (op == LOCK_EXCL) {
    return release_excl_lock(lock_id, requester ASYNC_ARGS);
  } else {
    return release_shared_lock(lock_id, requester ASYNC_ARGS);
  }
}

/**
 * Check if the lock is granted.
 * 
 * Returns true if the lock is granted, false otherwise.
 */
static inline bool is_excl_lock_granted(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  return cas_lock_obj(lm_of_lock(lock_id), lock_obj_raddr(lock_id), LOCK_OBJ(0, 0), LOCK_OBJ(requester, 0) ASYNC_ARGS);
}

static inline bool is_shared_lock_granted(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  lock_obj_t cur_state = read_lock_obj(lm_of_lock(lock_id), lock_obj_raddr(lock_id) ASYNC_ARGS);
  return EXCL_STATE(cur_state) == 0;
}

bool is_lock_granted(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS) {
  if (op == LOCK_EXCL) {
    return is_excl_lock_granted(lock_id, requester ASYNC_ARGS);
  } else {
    return is_shared_lock_granted(lock_id, requester ASYNC_ARGS);
  }
}

/**
 * Wait for the lock to be granted.
 * 
 * Returns 1 if the lock is granted.
 */
static inline int wait_for_excl_lock(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  while (!is_excl_lock_granted(lock_id, requester ASYNC_ARGS)) {;}
  return 1;
}

static inline int wait_for_shared_lock(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  while (!is_shared_lock_granted(lock_id, requester ASYNC_ARGS)) {;}
  return 1;
}

int caslock_wait_for_lock(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS) {
  if (op == LOCK_EXCL) {
    return wait_for_excl_lock(lock_id, requester ASYNC_ARGS);
  } else {
    return wait_for_shared_lock(lock_id, requester ASYNC_ARGS);
  }
}

/**
 * Abort the lock when lock acquisition timeout. 
 * 
 * If the lock is shared, release the shared lock 
 * since anyone attempts to acquire the shared lock will increase the shared state by one.
 * 
 * Returns 0.
 */
int abort_lock(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS) {
  if (op == LOCK_SHARED) {
    return release_shared_lock(lock_id, requester ASYNC_ARGS);
  } else {
    return 0;
  }
}