
#include <cstdint>
#include <list>

#include <junction/ConcurrentMap_Leapfrog.h>

#include "conf.h"
#include "lm.h"
#include "lc.h"
#include "remote.h"
#include "log.h"
#include "cohort.h"
#include "statistics.h"
#include "util.h"
#include "debug.h"

// const enum cohort_lock_policy policy = READER_PREFERRING;
const enum cohort_lock_policy policy = WRITER_PREFERRING;

junction::ConcurrentMap_Leapfrog<uint32_t, cohort_lock*> cht_lks;

/**
 * Helper function for accessing the lock object in the lock table.
 */
inline cohort_lock* lock_obj(uint32_t lid) {
  lid++; /* The hash map is buggy when the key is zero */

  // Try to find the lock object address from the map. If the
  // lock has not been initialized, allocate a new object.
  auto mutator = cht_lks.insertOrFind(lid);
  auto lk = mutator.getValue();
  if (!lk) {
    lk = new cohort_lock;
    lk->mtx.in = 0;
    lk->mtx.out = 0;
    auto old_val = mutator.exchangeValue(lk);

    // If two threads race on inserting the new object, the
    // latter should fail and we destruct the failed object.
    if (old_val) {
      LASSERT(old_val == lk);
      delete lk;
      lk = mutator.getValue();
    }
  }

  return lk;
}

/* The maximum allowed locally grant count. */
static int MAX_GRANT_CNT = 4;

void cohort_set_grant_cnt(int n) {
  MAX_GRANT_CNT = n;
}

#ifdef USE_TIMESTAMP

/**
 * Fetch the remote lock metadata and get the timestamp of the first
 * waiter. If there is no waiters, 0 is returned instead.
 */
uint32_t get_rwaiter_ts(uint32_t lid, uint16_t cid, char mode ASYNC_PARAMS) {
  auto rlock = (aql *)remote_lock_fetch(lid, cid ASYNC_ARGS);
  auto qhead = LQ_HEAD(rlock->hdr);
  auto qsize = LQ_SIZE(rlock->hdr);

  // Starting from the second lock participant, we find the
  // first queue entry whose version is valid and record its timestamp. 
  uint32_t ts = 0;
  for (int i = 1; i < qsize; i++) {
    auto qidx = (qhead + i) % MAX_QUEUE_SIZE;
    if (rlock->queue[qidx].ver == VERSION(qhead + i))
      if (!ts || ts_later(ts, rlock->queue[qidx].ts, TIMESTAMP_BITMASK))
        ts = rlock->queue[qidx].ts;
  }

  return ts;
}

// We let acquirers that can not get the local cohort lock instantly
// to update the remote waiter timestamp, as they can not make any
// progress anyway.
#define suspend_request(lk, lid, cid, shared, state, cmode) do {\
  cht_lqe e = {.client = cid, .shared = shared, \
                .ts = acquire_ts, .state = state};\
  lk->waiters.push_back(e);\
  if (shared) lk->sh_waiter_cnt++;\
  release_mutex(&lk->mtx);\
  if (!lk->rwaiter_ts) {\
    auto rts = get_rwaiter_ts(lid, cid, cmode ASYNC_ARGS);\
    if (rts && !lk->rwaiter_ts) lk->rwaiter_ts = rts;\
  }\
} while (0)

#else

#define suspend_request(lk, lid, cid, shared, state, cmode) do {\
  cht_lqe e = {.client = cid, .shared = shared, \
               .state = state};\
  lk->waiters.push_back(e);\
  if (shared) lk->sh_waiter_cnt++;\
  release_mutex(&lk->mtx);\
} while (0)

#endif

/**
 * Attempt to acquire the local cohort lock.
 * 
 * Returns CHT_GRANTED if the caller is the first thread that is granted
 * the lock (which is responsible for acquiring the remote lock).
 * 
 * Returns CHT_SHARED if the caller is directly granted the shared lock.
 * 
 * Returns CHT_SUSPENDED if the caller is suspended, and *granted will be
 * set to true when it is granted the lock. The caller is not
 * responsible of anything.
 */
int cohort_acquire(uint32_t lid, uint16_t cid, bool shared, char* state ASYNC_PARAMS) {
  auto lk = lock_obj(lid);
  uint32_t acquire_ts = acquire_timestamp[cid];

  // If the lock is free, grant the cohort lock and ask the caller
  // to acquire the remote lock subsequently.
  acquire_mutex_of_lock(&lk->mtx, lid, cid ASYNC_ARGS);
  if (lk->state == COHORT_FREE) {
    lk->state = COHORT_PENDING;
    lk->holder_cnt = 1;
    lk->sh_waiter_cnt = 0;
    lk->grant_cnt = 1;
    lk->rwaiter_ts = 0;
    *state = WAIT_REMOTE_LOCK;
    release_mutex(&lk->mtx);
    return CHT_GRANTED;
  }

  // We can grant new shared requests to a shared lock
  // as long as it does not harm fairness.
  if (lk->state == COHORT_SHARED && shared) {

#ifdef USE_TIMESTAMP
    // If there is no waiters from other CNs that come earlier 
    // than us, grant the request.
    if (!lk->rwaiter_ts ||
         ts_later(lk->rwaiter_ts, acquire_ts, TIMESTAMP_BITMASK)) {
#else
    // If the grant count has not exceeded the maximum,
    // grant the request.
    if (!MAX_GRANT_CNT || lk->grant_cnt < MAX_GRANT_CNT) {
      lk->grant_cnt++;
#endif
      lk->holder_cnt++;
      *state = GRANTED;
      release_mutex(&lk->mtx);
      add_counter(TID_CORE(cid), CTR_LOCAL_GRANT);
      return CHT_SHARED;

    // If there are waiters from other CNs that come earlier than us,
    // we need to stop granting local requests for fairness.
    } else {
      suspend_request(lk, lid, cid, shared, state, lk->state);
      return CHT_SUSPENDED;
    }

  // Suspend the request if the lock or the request is exclusive.
  } else {
    if (policy == WRITER_PREFERRING && lk->state == COHORT_SHARED && !shared)
      lk->state = COHORT_WWAIT;
    suspend_request(lk, lid, cid, shared, state, lk->state);
    return CHT_SUSPENDED;
  }
}

/**
 * Should only be called after receiving the remote lock ownership.
 */
void cohort_after_grant(uint32_t lid, uint16_t cid, uint32_t remote_ret, bool shared ASYNC_PARAMS) {

  // If there are nothing to do, just return.
  auto rwaiter_ts = remote_ret >> 1;
  if (!remote_ret || (!shared && !rwaiter_ts)) return;

  auto lk = lock_obj(lid);
  acquire_mutex_of_lock(&lk->mtx, lid, cid ASYNC_ARGS);

#ifdef USE_TIMESTAMP
  // Update the remote waiter timestamp passed by the previous holder.
  if (rwaiter_ts) lk->rwaiter_ts = rwaiter_ts;
#endif

  // Update the lock state to grant subsequent shared requests.
  if (shared) {
    lk->state = COHORT_SHARED;

    // Grant the cohort lock to other pending shared requests
    // that are allowed to be granted locally.
    auto e = lk->waiters.begin();
    for (; e != lk->waiters.end(); ) {
      if (!e->shared) {
        lk->state = COHORT_WWAIT;
        e++;
        #ifdef TASK_FAIR
          break;
        #endif
      } else {
        lk->holder_cnt++;
        lk->grant_cnt++;
        *(e->state) = GRANTED;
        e = lk->waiters.erase(e);
        add_counter(TID_CORE(cid), CTR_LOCAL_GRANT);
        lk->sh_waiter_cnt--;
      }

      #ifdef USE_TIMESTAMP
        #ifdef TASK_FAIR
          if (lk->rwaiter_ts &&
              ts_later(e->ts, lk->rwaiter_ts, TIMESTAMP_BITMASK)) break;
        #endif
      #else
        if (MAX_GRANT_CNT && lk->grant_cnt >= MAX_GRANT_CNT) break;
      #endif
    }
  }

  release_mutex(&lk->mtx);
}
 
/**
 * Release the local cohort lock.
 */
int cohort_release(uint32_t lid, uint16_t cid, bool shared ASYNC_PARAMS) {
  auto lk = lock_obj(lid);
  acquire_mutex_of_lock(&lk->mtx, lid, cid ASYNC_ARGS);

  // Shared holder releases, we let the last holder do the lock 
  // scheduling job.
  if (lk->holder_cnt > 1) {
    lk->holder_cnt--;
    release_mutex(&lk->mtx);
    return 0;
  }

  // Now all current holders have released the lock,
  // if there are no waiters, release the remote lock.
  LASSERT(lk->holder_cnt == 1);
  if (lk->waiters.size() == 0) {
    lk->state = COHORT_FREE;
    remote_rw_unlock(lid, cid, shared ? LOCK_SHARED : LOCK_EXCL ASYNC_ARGS);
    release_mutex(&lk->mtx);

  // We allow local schedule between requests with the same mode
  // as long as the waiter comes earlier than those on other CNs.
  } else {
    auto waiter = lk->waiters.begin();
    auto local_grant = false;

    // For shared->exclusive or exclusive->shared transitions,
    // the remote lock must be released and reacquired.
    if (shared != waiter->shared) {
      local_grant = false;

    // For exclusive->exclusive transitions, hand over the lock
    // locally as long as there are no remote waiters that came earlier.
    } else {
      #ifdef USE_TIMESTAMP
        local_grant = !lk->rwaiter_ts || 
                      ts_later(lk->rwaiter_ts, waiter->ts, TIMESTAMP_BITMASK);
      #else
        local_grant = !MAX_GRANT_CNT || lk->grant_cnt < MAX_GRANT_CNT;
        lk->grant_cnt++;
      #endif

      // Shared-shared transition is possible if local grants are blocked
      // by remote waiters that came earlier.
      if (shared) ASSERT_MSG(!local_grant, 
        "host%d tid%d lock%d remote ts %d, local ts %d",
        LHID, cid, lid, lk->rwaiter_ts, waiter->ts);

      // If we can not grant the lock locally, try to schedule the lock to a
      // reader in waiters to improve the concurrency.
      #ifndef TASK_FAIR
        if (!local_grant && lk->sh_waiter_cnt > 0) {
          for (; waiter != lk->waiters.end(); waiter++)
            if (waiter->shared) break;
          LASSERT(waiter != lk->waiters.end());
        }
      #endif
    }

    // Local handover.
    if (local_grant) {
        *(waiter->state) = GRANTED;
        lk->waiters.erase(waiter);
        if (waiter->shared) lk->sh_waiter_cnt--;
        release_mutex(&lk->mtx);

    // Transfer lock ownership to remote waiters and 
    // let the selected waiter reacquire the lock.
    } else {
      char* waiter_state = waiter->state;
      lk->state = COHORT_PENDING;
      lk->grant_cnt = 1;
      lk->rwaiter_ts = 0;
      lk->waiters.erase(waiter);
      if (waiter->shared) lk->sh_waiter_cnt--;
      release_mutex(&lk->mtx);

      remote_rw_unlock(lid, cid, shared ? LOCK_SHARED : LOCK_EXCL ASYNC_ARGS);
      *(waiter_state) = REACQUIRE_REMOTE_LOCK;
    }

    add_counter(TID_CORE(cid), CTR_LOCAL_GRANT);
  }

  return 0;
}

/**
 * Abort a request that is not granted.
 */
int cohort_abort(uint32_t lid, uint16_t cid, bool shared, bool waiting ASYNC_PARAMS) {
  auto lk = lock_obj(lid);
  acquire_mutex_of_lock(&lk->mtx, lid, cid ASYNC_ARGS);

  // Remove the pending request from the wait queue.
  if (waiting) {
    auto& wq = lk->waiters;
    for (auto e = wq.begin(); e != wq.end(); e++) {
      if (e->client == cid) {
        wq.erase(e);
        release_mutex(&lk->mtx);
        return 0;
      }
    }

    // If the request is not found in the wait queue, the
    // abort should have raced with the reacquire. In this case,
    // we let the next waiter reacquire instead.
    if (lk->waiters.size() == 0) {
      lk->state = COHORT_FREE;
    } else {
      *(lk->waiters.front().state) = REACQUIRE_REMOTE_LOCK;
      lk->waiters.pop_front();
    }
    release_mutex(&lk->mtx);

  // Let the next waiter re-acquire the remote lock.
  } else {
    cht_lqe e;
    if (lk->waiters.size() != 0) {
      *(lk->waiters.front().state) = REACQUIRE_REMOTE_LOCK;
      lk->waiters.pop_front();

    // If there is no waiter, free the cohort lock.
    } else {
      lk->state = COHORT_FREE;
      lk->grant_cnt = 0;
      lk->rwaiter_ts = 0;
    }
  }

  release_mutex(&lk->mtx);
  return 0;
}
