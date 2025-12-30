#include <vector>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <chrono>

#include "rdma.h"
#include "lcore.h"
#include "lm.h"
#include "lc.h"
#include "util.h"
#include "debug.h"
#include "conf.h"
#include "sconf.h"
#include "statistics.h"
#include "random.h"

using std::vector;
using std::unordered_map;
using std::mutex;
using std::lock_guard;
using std::min;

using namespace std::chrono;
using namespace std::chrono_literals;

static Random random_gen[MAX_CORE_NUM];

/**
 * Configurations and variables for backoff.
*/
#define COUNT_MAX       32768
#define BASE_BACKOFF    100
#define MAX_BACKOFF     (32 * BASE_BACKOFF)

uint64_t t_max[MAX_CORE_NUM];
std::atomic<uint64_t> retry_cnt;
std::atomic<uint64_t> acquire_cnt;

#define TMAX_CHANGE_INTERVAL  100
steady_clock::time_point last_tmax_change;

/**
 * Static configurations, confirmed on setup.
 */
static vector<uint16_t> memory_nodes;
static size_t lock_num;

/**
 * Variables and APIs to handle counter overflow.
*/
unordered_map<uint16_t, unordered_map<uint32_t, unordered_map<uint32_t, lock_obj_t>>> reset_from;

static inline void set_reset_from(uint32_t lock_id, uint32_t requester, lock_obj_t val) {
  reset_from[get_cur_lcore_id()][lock_id][requester] = val;
}

static inline lock_obj_t get_reset_from_set_zero(uint32_t lock_id, uint32_t requester) {
  uint16_t lcore_id = get_cur_lcore_id();
  lock_obj_t res = reset_from[lcore_id][lock_id][requester];
  reset_from[lcore_id][lock_id][requester] = 0;
  return res;
}

/**
 * A map to record the `prev` variable got from `dslr_acquire`,
 * which will be used by `dslr_release`.
*/
unordered_map<uint16_t, unordered_map<uint32_t, unordered_map<uint32_t, lock_obj_t>>> prev_map;

static inline void set_prev(uint32_t lock_id, uint32_t requester, lock_obj_t prev) {
  prev_map[get_cur_lcore_id()][lock_id][requester] = prev;
}

static inline lock_obj_t get_prev_set_zero(uint32_t lock_id, uint32_t requester) {
  uint16_t lcore_id = get_cur_lcore_id();
  lock_obj_t res = prev_map[lcore_id][lock_id][requester];
  prev_map[lcore_id][lock_id][requester] = 0;
  return res;
}

/**
 * Util functions to partition lock among
 * memory nodes.
*/
static inline size_t lock_obj_raddr(uint32_t lock_id) {
  return (size_t)(lock_id * sizeof(lock_obj_t));
}

/**
 * Implement the RadndomBackoff process in DSLR paper.
 * The wait time is drawn uniformly at random from the
 * following interval:
 * [0, min(R*(2^(c-1)), L)]
 * Where R is BASE_BACKOFF_US in our implementation,
 * c is the number of retry,
 * L is MAX_BACKOFF_US in our implementation.
*/
static inline int random(int max) {
  return random_gen[get_cur_lcore_id()].next_uniform() % max;
}

void delay_w_yield(uint64_t nanosec ASYNC_PARAMS) {
  const auto timeout = nanoseconds(nanosec);
  auto start = steady_clock::now();
  while (steady_clock::now() - start < timeout) YIELD;
}

/**
 * Check whether the counters in the lock object overflow.
 * Return 1 if already / is going to overflow,
 * otherwise return 0.
 * 
 * If already overflow, do the following things:
 * 1. Withdraw the lock request.
 * 2. Perform RandomBackoff until the counters have been reset.
 * 3. Return 1.
 * 
 * If is going to overflow, do the following things:
 * 1. Set a flag and record the `reset_from`, so that
 * current txn can reset the counters when releasing the lock.
 * 2. Return 1.
*/
static inline int check_counter_overflow(uint32_t lock_id, uint8_t op, 
  uint32_t requester, lock_obj_t& prev ASYNC_PARAMS) {
  if (SHARED_MAX(prev) >= COUNT_MAX || EXCL_MAX(prev) >= COUNT_MAX) {
    // The counters already overflow.

    // Withdraw the request.
    if (op == LOCK_SHARED) {
      prev = DEC_SHARED_MAX(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
    } else {
      prev = DEC_EXCL_MAX(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
    }

    int c = 1;
    do {
      // Perform RandomBackoff.
      // random_backoff(c);
      c++;
      prev = read_lock_obj(lm_of_lock(lock_id), lock_obj_raddr(lock_id) ASYNC_ARGS);
    } while (SHARED_MAX(prev) >= (COUNT_MAX - 1) || EXCL_MAX(prev) >= (COUNT_MAX - 1));

    // Acquire the lock again.
    if (op == LOCK_SHARED) {
      prev = FA_SHARED_MAX(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
    } else {
      prev = FA_EXCL_MAX(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
    }

    return 1; 
  } else if (op == LOCK_SHARED && SHARED_MAX(prev) == (COUNT_MAX - 1)) {
    set_reset_from(lock_id, requester, 
      LOCK_OBJ(EXCL_MAX(prev), COUNT_MAX, EXCL_MAX(prev), COUNT_MAX));

    return 1;
  } else if (op == LOCK_EXCL && EXCL_MAX(prev) == (COUNT_MAX - 1)) {
    set_reset_from(lock_id, requester, 
      LOCK_OBJ(COUNT_MAX, SHARED_MAX(prev), COUNT_MAX, SHARED_MAX(prev)));

    return 1;
  }

  // No overflow already / is going to occur.
  return 0;
}

/**
 * Wait until the lock can be granted to current requester.
 * Return success(1) or failure(-1).
*/
static inline int handle_conflict(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS) {
  lock_obj_t prev = get_prev_set_zero(lock_id, requester);
  uint64_t t_back = BASE_BACKOFF;  /* Current backoff time */
  auto core = get_cur_lcore_id();

  while (1) {
    lock_obj_t val = read_lock_obj(lm_of_lock(lock_id), lock_obj_raddr(lock_id) ASYNC_ARGS);
    if (op == LOCK_SHARED) {
      if (EXCL_MAX(prev) == EXCL_CNT(val)) {
        return 1;
      }
    } else if (op == LOCK_EXCL) {
      if (EXCL_MAX(prev) == EXCL_CNT(val)
        && SHARED_MAX(prev) == SHARED_CNT(val)) {
        return 1;
      }
    }

    // int wait_count = (EXCL_MAX(prev) - EXCL_CNT(val)) + 
    //                  (SHARED_MAX(prev) - SHARED_CNT(val));
    // delay_w_yield(wait_count * BASE_BACKOFF ASYNC_ARGS);

    if (!t_max[core]) t_max[core] = MAX_BACKOFF;

    // Dynamic backoff limit.
    auto tmax_dur = duration_cast<microseconds>(
      steady_clock::now() - last_tmax_change).count();
    auto rcnt = retry_cnt.fetch_add(1);

    if (last_tmax_change.time_since_epoch().count() == 0 ||
      tmax_dur > TMAX_CHANGE_INTERVAL) {

      auto acnt = acquire_cnt.load();
      double retry_ratio = (double)rcnt / (rcnt + acnt);
      if (retry_ratio > 0.7 && t_max[core] < MAX_BACKOFF) t_max[core] *= 2;
      if (retry_ratio < 0.5 && t_max[core] > BASE_BACKOFF) t_max[core] /= 2;
      last_tmax_change = steady_clock::now();
    }

    // Exponential backoff with random offset.
    auto t = (t_back < t_max[core]) ? t_back : t_max[core];
    auto t_final = t + random(2000);
    // LLOG("[lock%d][core%d] t_max: %d us, t_final: %d ns, retry_ratio: %.2f", 
    //   lock_id, core, t_max[core], t_final, retry_ratio);
    delay_w_yield(t_final ASYNC_ARGS);
    t_back *= 2;
  }
  return -1;
}

/**
 * The logic of acquiring shared lock.
*/
static inline int acquire_shared_lock(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  lock_obj_t prev = FA_SHARED_MAX(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
  check_counter_overflow(lock_id, LOCK_SHARED, requester, prev ASYNC_ARGS);

  if (EXCL_CNT(prev) == EXCL_MAX(prev)) {
    // Lock granted.
    return 1;
  } else {
    // Lock is not granted.
    // Record the `prev` variable.
    set_prev(lock_id, requester, prev);
    return 0;
  }
}

/**
 * The logic of acquiring exclusive lock.
*/
static inline int acquire_exclusive_lock(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  lock_obj_t prev = FA_EXCL_MAX(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
  check_counter_overflow(lock_id, LOCK_EXCL, requester, prev ASYNC_ARGS);

  if (EXCL_CNT(prev) == EXCL_MAX(prev)
    && SHARED_CNT(prev) == SHARED_MAX(prev)) {
    // Lock granted.
    return 1;
  } else {
    // Lock is not granted.
    // Record the `prev` variable.
    set_prev(lock_id, requester, prev);
    return 0;
  }
}

/**
 * Acquire a lock asynchronously.
 * Returns 1 if the lock is granted, 
 * 0 if the request is suspended,
 * -1 if the request fails.
 */
int dslr_acquire(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS) {
  acquire_cnt.fetch_add(1);
  if (op == LOCK_SHARED) {
    return acquire_shared_lock(lock_id, requester ASYNC_ARGS);
  } else {
    return acquire_exclusive_lock(lock_id, requester ASYNC_ARGS);
  }
}

static inline int release_shared_lock(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  lock_obj_t val = FA_SHARED_CNT(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
  return 1;
}

static inline int release_exclusive_lock(uint32_t lock_id, uint32_t requester ASYNC_PARAMS) {
  lock_obj_t val = FA_EXCL_CNT(lm_of_lock(lock_id), lock_obj_raddr(lock_id));
  return 1;
}

/**
 * Release a lock.
 * Always return success(1).
*/
int dslr_release(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS) {
  if (op == LOCK_SHARED) {
    release_shared_lock(lock_id, requester ASYNC_ARGS);
  } else {
    release_exclusive_lock(lock_id, requester ASYNC_ARGS);
  }

  lock_obj_t expected = get_reset_from_set_zero(lock_id, requester);
  if (expected == 0) {
    // There is no overflow.
    return 1;
  }
  // Reset overflown counter.
  bool cas_success = false;
  while (!cas_success) {
    cas_success = cas_lock_obj(lm_of_lock(lock_id), lock_obj_raddr(lock_id), expected, 0 ASYNC_ARGS);
  }
  return 1;
}

/**
 * Wait for the lock granted.
 * Always return success(1), or failure(-1).
*/
int dslr_wait_for_lock(uint32_t lock_id, uint8_t op, uint32_t requester ASYNC_PARAMS) {
  int ret = handle_conflict(lock_id, op, requester ASYNC_ARGS);
  ASSERT_MSG(ret != -1 && ret != 0, 
    "[host%u][core%u]handle_conflict error",
    LHID, get_cur_lcore_id());
  return ret;
}

int dslr_setup_lc(size_t lk_num, bitmap core_map) {
  lock_num = lk_num;

  // Init reset_from table and prev_map for each lcore.
  for (uint16_t i = 0; i < MAX_CORE_NUM; i++) {
    if (BITMAP_CONTAIN(core_map, i)) {
      reset_from[i] = unordered_map<uint32_t, unordered_map<uint32_t, lock_obj_t>>{};
      prev_map[i] = unordered_map<uint32_t, unordered_map<uint32_t, lock_obj_t>>{};
    }
  }

  return 0;
}

// TODO
bool is_lock_granted(uint32_t lock_id, uint8_t op, uint32_t requester) {
  return false;
}

int abort_lock(uint32_t lock_id, uint8_t op, uint32_t requester) {
  return 0;
}