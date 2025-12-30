
#include <vector>
#include <atomic>
#include <unordered_set>
#include <unordered_map>
#include <cstdint>

#include <junction/ConcurrentMap_Leapfrog.h>

#include "common.h"
#include "conf.h"
#include "log.h"
#include "lc.h"
#include "statistics.h"
#include "remote.h"
#include "lm.h"
#include "rdma.h"

using std::vector;
using std::unordered_set;
using std::unordered_map;
using namespace std::chrono;

#define RDMA_WAIT_ONE(mn, tid) \
  do { YIELD; } while (!rdma_poll_one(mn, tid))

template <class T>
struct LockKeyTraits {
    typedef T Key;
    typedef typename turf::util::BestFit<T>::Unsigned Hash;
    static const Key NullKey = Key(0xffffffff);
    static const Hash NullHash = Hash(0xffffffff);
    static Hash hash(T key) {
        return turf::util::avalanche(Hash(key));
    }
    static Key dehash(Hash hash) {
        return (T) turf::util::deavalanche(hash);
    }
};

template <class T>
struct TsValueTraits {
    typedef T Value;
    typedef typename turf::util::BestFit<T>::Unsigned IntType;
    static const IntType NullValue = 0;
    static const IntType Redirect = 0xffffffff;
};

#define NO_TS 0x1
junction::ConcurrentMap_Leapfrog<uint32_t, uint32_t, 
  LockKeyTraits<uint32_t>, TsValueTraits<uint32_t>>
  granted_locks[MAX_CLIENT_NUM];

typedef struct {
  uint32_t tid;
} wake_waiter_args;

#ifndef USE_COHORT
static const bool local_notify = false;
#else
static const bool local_notify = false;
#endif

/**
 * Lock reset.
 */
unordered_set<uint32_t> rst_locks[MAX_CLIENT_NUM];
junction::ConcurrentMap_Leapfrog<uint32_t, uint32_t,
  LockKeyTraits<uint32_t>, TsValueTraits<uint32_t>> rst_cnt;

/**
 * Macros for local buffer manipulation.
 */
#define SENDBUF_LMETA_LOCK(buf) ((char *)buf)
#define SENDBUF_LMETA_UNLOCK(buf) ((char *)buf + 8)
#define SENDBUF_LQE_LOCK(buf) ((char *)buf + 16)
#define SENDBUF_LQE_UNLOCK(buf) ((char *)buf + 24)
#define SENDBUF_LOCK_QUEUE(buf) ((char *)buf + 32)

/**
 * Fill the LQE at `idx` asynchronously.
 */
#ifdef USE_TIMESTAMP
#define FILL_LQE_SET_TS(time)  _entry->ts = time
#else
#define FILL_LQE_SET_TS(time)  (0)
#endif

#define FILL_LQE(lock, tid, idx, ope, buf) do {\
  auto _entry = (lqe *)SENDBUF_LQE_LOCK(buf);\
  _entry->op = ope;\
  _entry->mid = LHID;\
  _entry->tid = tid;\
  _entry->ver = VERSION(idx);\
  FILL_LQE_SET_TS(ts_acq);\
  \
  LC_DEBUG("fill lqe: addr %lx, op %d, idx %ld, mid %d, tid %d, version %d", \
    LQE_ADDR(lock, (idx) % MAX_QUEUE_SIZE), ope, idx, LHID, tid, _entry->ver);\
  rdma_write(tid, lm_of_lock(lock), (char *)_entry, \
    LQE_ADDR(lock, (idx) % MAX_QUEUE_SIZE), sizeof(lqe), false);\
  RDMA_WAIT_ONE(lm_of_lock(lock), tid);\
} while (0)

/**
 * Fetch the LQE at `idx` to `entry`.
 * 
 * When we fetch the lock queue entry, it is possible that the waiter 
 * has not written its information into the entry. We can tell from 
 * the sequence number we read. If the sequence number correctly 
 * identifies the number of loops in the ring buffer, the entry is valid. 
 * This macro retries fetching until the entry is valid.
 */
#define FETCH_LQE(lock, tid, entry, idx, hdr) do {\
  for (int _t = 0; true; _t++) {\
    add_counter(TID_CORE(tid), CTR_LQ_FETCH);\
    rdma_read(tid, lm_of_lock(lock), (char *)(entry), \
      LQE_ADDR(lock, (idx) % MAX_QUEUE_SIZE), sizeof(lqe), false);\
    RDMA_WAIT_ONE(lm_of_lock(lock), tid);\
    \
    LC_DEBUG("fetch lqe: addr %lx idx %ld, op %d, mid %d, tid %d, version %d", \
      LQE_ADDR(lock, (idx) % MAX_QUEUE_SIZE), idx, \
      (entry)->op, (entry)->mid, (entry)->tid, (entry)->ver);\
    \
    if ((entry)->ver == BITMASK(VERSION_BITS) - 1 || \
      ((entry)->ver != BITMASK(VERSION_BITS) && \
       (entry)->ver > VERSION(idx)) || _t > MAX_FETCH_COUNT) {\
      remote_lock_reset(lock, tid, hdr ASYNC_ARGS);\
      return;\
    }\
    if ((entry)->ver == VERSION(idx)) break;\
  }\
} while (0)

/**
 * Notify the waiter stored in entry `head` in the lock queue.
 */
#ifdef USE_TIMESTAMP
#define GET_NEXT_WAITER_TS(idx, num) ({\
  uint32_t _ts = 0;\
  for (int _i = 0; _i < LQ_SIZE(meta) - 1 && _i < lookup_entry_num(); _i++) {\
    auto next_waiter = &queue[(idx + num + _i) % MAX_QUEUE_SIZE];\
    if (next_waiter->ver == VERSION(idx + num + _i)) \
      if (!_ts || ts_later(_ts, next_waiter->ts, TIMESTAMP_BITMASK))\
        _ts = next_waiter->ts;\
  }\
  (_ts);\
})
#define NOTIFY_WAITERS_SET_TS(ts) _buf->rwaiter_ts = ts
#define TS(ts) (((ts) << 1) ^ 1)
#else
#define GET_NEXT_WAITER_TS(idx, num) (0)
#define NOTIFY_WAITERS_SET_TS(ts) (0)
#define TS(ts) NO_TS
#endif

#define SET_NOTIF_TID(id) _buf->tid = id

#define NOTIFY_WAITERS(lock, tid, queue, idx, num, nrst) do {\
  auto _ts = GET_NEXT_WAITER_TS(idx, num);\
  for (int _i = 0; _i < num; _i++) {\
    auto entry = &queue[(idx + _i) % MAX_QUEUE_SIZE];\
    if (local_notify && entry->mid == LHID) {\
      LC_DEBUG("notify local waiter: tid %d", entry->tid);\
      granted_locks[entry->tid].assign(lock, TS(_ts));\
    } else {\
      bool _sync;\
      auto _buf = (aql_notification *)rdma_sendbuf(tid,\
                    sizeof(aql_notification), &_sync);\
      _buf->lock = lock;\
      _buf->rst = 0;\
      _buf->ack = 0;\
      _buf->rst_cnt = nrst;\
      NOTIFY_WAITERS_SET_TS(_ts);\
      SET_NOTIF_TID(entry->tid);\
      \
      LC_DEBUG("notify waiter: mid %d, tid %d, rst_cnt %d", \
        entry->mid, entry->tid, _buf->rst_cnt);\
      rdma_send(TID_CORE(tid), entry->mid, TID_CORE(entry->tid), \
        (char *)_buf, sizeof(aql_notification), _sync);\
    }\
  }\
} while (0)

/**
 * Utility functions.
 */
void delay_w_yield(uint64_t nanosec ASYNC_PARAMS) {
  const auto timeout = nanoseconds(nanosec);
  auto start = steady_clock::now();
  while (steady_clock::now() - start < timeout) YIELD;
}

int wake_waiter(char* buf, qkey from, void* args) {
  auto notif = (aql_notification *)buf;
  auto wwargs = (wake_waiter_args *)args;
  auto core = TID_CORE(wwargs->tid);
  auto lock = notif->lock;
  auto tid = wwargs->tid;
  auto nrst = rst_cnt.insertOrFind(notif->lock);

  // Upon receiving a reset signal, mark the lock as reset
  // and increase the local reset count.
  if (notif->rst) {
    for (int i = 0; i < CRT_NUM; i++)
      rst_locks[MAKE_TID(core, i)].insert(notif->lock);
    nrst.assignValue(notif->rst_cnt);
    LC_DEBUG("received reset signal, rst cnt -> %d", notif->rst_cnt);
    return 0;
  }

  // Ignore notifications sent before the reset.
  if (notif->rst_cnt < nrst.getValue()) {
    LC_DEBUG("ignored notification for tid %d, rst cnt %d", 
      notif->tid, notif->rst_cnt);
    return 0;
  }

  LC_DEBUG("received notification for tid %d, rst cnt %d", 
    notif->tid, notif->rst_cnt);
  granted_locks[notif->tid].assign(notif->lock, TS(notif->rwaiter_ts));
  return 0;
}

inline int lookup_entry_num() {
  return 1;
  // static int bias_cnt = 0;
  // int cn_num = CN_LIST.size();
  // int bias_bar = cn_num % 4;
  // int bias = (bias_cnt < bias_bar) ? 1 : 0;
  // int n = cn_num / 4 + bias;
  // bias_cnt = (bias_cnt + 1) % 4;
  // if (n < 1) return 1;
  // else return n;
}

bool digest_reset_notifications(uint32_t lock, uint32_t tid ASYNC_PARAMS) {
  bool reset = false;

  // Process all buffered reset notifications to this client.
  while (!rst_locks[tid].empty()) {
    auto lk = rst_locks[tid].begin();

    // If the notification is relevant to the lock that we
    // are acquiring, tell the caller.
    if (*lk == lock) reset = true;

    // Reply to the notification.
    auto hdr = (uint64_t *)SENDBUF_LMETA_LOCK(rdma_rc_buf(tid));
    rdma_fa(tid, lm_of_lock(*lk), (char *)hdr, HDR_ADDR(*lk), 
      RST_LMETA(1), false);
    RDMA_WAIT_ONE(lm_of_lock(*lk), tid);
    LC_DEBUG("increase reset cnt of lock%d, retval 0x%lx, rst cnt %d", 
      *lk, RST_CNT(*hdr), rst_cnt.get(*lk));

    // Remove the notification.
    rst_locks[tid].erase(lk);
  }

  return reset;
}

bool check_reset(uint32_t lock, uint32_t tid, uint64_t* hdr, 
                 bool held ASYNC_PARAMS) {
  
  // If the lock is being reset, wait until the reset finishes.
  if (RST_BIT(*hdr)) {
    LC_DEBUG("lock reset during %s", held ? "release" : "acquire");

    while (RST_BIT(*hdr)) {
      rdma_read(tid, lm_of_lock(lock), (char *)hdr, 
        HDR_ADDR(lock), 8, false);
      RDMA_WAIT_ONE(lm_of_lock(lock), tid);

      // When waiting for the reset to finish, regularly check
      // the reset notification and digest it.
      digest_reset_notifications(lock, tid ASYNC_ARGS);
      wake_waiter_args arg = {.tid = tid};
      rdma_poll_recvs(TID_CORE(tid), wake_waiter, (void *)(&arg));
    }

    // We need to remove all grant notifications for the lock.
    for (int i = 0; i < MAX_CLIENT_NUM; i++)
      granted_locks[i].erase(lock);

    return true;
  }

  return false;
}

void acquire_mutex_of_lock(cohort_mutex* mtx, uint32_t lid, 
                          uint32_t cid ASYNC_PARAMS) {
  uint16_t ticket = __atomic_fetch_add(&mtx->in, 1, __ATOMIC_SEQ_CST);
  while (ticket != __atomic_load_n(&mtx->out, __ATOMIC_RELAXED)) {
    digest_reset_notifications(lid, cid ASYNC_ARGS);
    YIELD;
  }
}

void release_mutex(cohort_mutex* mtx) {
  __atomic_fetch_add(&mtx->out, 1, __ATOMIC_SEQ_CST);
}

/**
 * Acquire a remote read/write lock asynchronously.
 * 
 * Returns 1 if the lock is granted, 0 if the request is suspended.
 * 
 * reqs is an array containing RDMA requests that are batched
 * with the lock release request to hide network latency.
 * They will be executed after the lock acquire.
 * 
 * The capacity of `reqs` should be nreq+1. Moreover, the first
 * entry of `reqs` is reserved for the acquire request.
 */
int remote_rw_lock(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS) {
  auto core = TID_CORE(tid);
  auto buf = rdma_rc_buf(tid);
  auto old_meta = (uint64_t *)SENDBUF_LMETA_LOCK(buf);
  auto ts_acq = acquire_timestamp[tid];

  // Based on the mutex version, we add excl_cnt to record the
  // number of exclusive holders and waiters. Exclusive requests
  // adds excl_cnt by 1, shared requests does not affect it.
  uint32_t excl_add = (op == LOCK_EXCL) ? 1 : 0;
  rreq_desc reqs[1] = {
    {
      .op = IBV_WR_ATOMIC_FETCH_AND_ADD, .len = 8, 
      .local_addr = (uint64_t)old_meta, .remote_addr = HDR_ADDR(lock),
      .compare_add = LMETA(excl_add, 0, 1), .swap = 0
    },
  };

  rdma_one_sided_batch(tid, lm_of_lock(lock), 1, reqs);
  RDMA_WAIT_ONE(lm_of_lock(lock), tid);

  auto meta = *old_meta;
  LC_DEBUG("rwlock acquire op %d, old_meta: excl %ld, qhead %ld, qsize %ld", 
    op, EXCL_CNT(meta), LQ_HEAD(meta), LQ_SIZE(meta));

  // Re-acquire the lock if the lock is reset.
  if (check_reset(lock, tid, old_meta, false ASYNC_ARGS))
    return remote_rw_lock(lock, tid, op ASYNC_ARGS);

  // Reset the lock if the queue overflows.
  if (LQ_SIZE(meta) >= LQ_CAPACITY) {
    remote_lock_reset(lock, tid, old_meta ASYNC_ARGS);
    return remote_rw_lock(lock, tid, op ASYNC_ARGS);
  }

  // If the lock can be granted, directly return.
  if (LQ_SIZE(meta) == 0 || 
     (op == LOCK_SHARED && EXCL_CNT(meta) == 0)) return 1;

  // Otherwise, record the requester information in the queue.
  FILL_LQE(lock, tid, LQ_HEAD(meta) + LQ_SIZE(meta), op, buf);
  return 0;
}

/**
 * Release a remote read/write lock.
 * 
 * reqs is an array containing RDMA requests that are batched
 * with the lock release request to hide network latency.
 * They will be executed before the lock release.
 * 
 * The capacity of reqs should be nreq+2.
 */
void remote_rw_unlock(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS) {
  auto core = TID_CORE(tid);
  auto buf = rdma_rc_buf(tid);
  auto old_meta = (uint64_t *)SENDBUF_LMETA_UNLOCK(buf);
  auto queue = (lqe *)SENDBUF_LOCK_QUEUE(buf);
  auto nrst = rst_cnt.get(lock);

  // Pop the first lock queue entry.
  // 
  // Here we want to realize excl_cnt--, lq_head++, lq_size-- simultaneously.
  // This is tricky, because the carry bit of the three fields interferes
  // with each other. excl_cnt-- produces a carry bit to lq_size, so lq_size--
  // should be lq_size -= 2. lq_size -= 2 also produces a carry bit, so
  // lq_head++ should be lq_head += 0.
  // 
  // Note that if we are the only exclusive holder, the lock is grantable
  // for shared holders once this FA operation finishes. Hence, we only
  // need to grant the lock to shared waiters in the current lock queue.
  uint64_t meta_add = (op == LOCK_EXCL) ? LMETA(-1, 0, -2) : LMETA(0, 0, -1);
  rreq_desc reqs[2] = {
    {
      .op = IBV_WR_ATOMIC_FETCH_AND_ADD, .len = 8, 
      .local_addr = (uint64_t)old_meta, .remote_addr = HDR_ADDR(lock),
      .compare_add = meta_add, .swap = 0
    },

    // Prefetch the complete lock queue in the same batch.
    {
      .op = IBV_WR_RDMA_READ, .len = (uint32_t)(LQ_CAPACITY * sizeof(lqe)),
      .local_addr = (uint64_t)queue, .remote_addr = LQE_ADDR(lock, 0),
      .compare_add = 0, .swap = 0
    },
  };

  rdma_one_sided_batch(tid, lm_of_lock(lock), 2, reqs);
  RDMA_WAIT_ONE(lm_of_lock(lock), tid);
 
  // Report received metadata.
  auto meta = *old_meta;
  LC_DEBUG("rwlock release op %d, old_meta: excl %ld, qhead %ld, qsize %ld", 
    op, EXCL_CNT(meta), LQ_HEAD(meta), LQ_SIZE(meta));

  if (check_reset(lock, tid, old_meta, true ASYNC_ARGS)) return;

  // Examine the lock queue if there are more participants.
  if (LQ_SIZE(meta) > 1) {
    auto head = LQ_HEAD(meta) + 1;
    auto entry = &queue[head % MAX_QUEUE_SIZE];

    // If the shared holder releases the lock, we need to know whether
    // the next queue entry is a shared holder or an exclusive waiter.
    if (op == LOCK_SHARED) {

      // Fast path: all holders are shared.
      if (!EXCL_CNT(meta)) return;

      // Fast path: the next queue entry is valid.
      if (entry->ver == VERSION(head)) {
        if (entry->op == LOCK_EXCL)
          NOTIFY_WAITERS(lock, tid, queue, head, 1, nrst);
        return;
      }

      // Since shared holders might not write their information into the
      // queue slot, we need to determine the position of exclusive waiters 
      // in the queue.
      uint32_t excnt = 0, loop_cnt = 0;
      while (excnt != EXCL_CNT(meta)) {
        for (int i = 0; i < LQ_SIZE(meta) - 1; i++) {
          auto entry = &queue[(head + i) % MAX_QUEUE_SIZE];
          if (entry->ver == VERSION(head + i) && 
              entry->op == LOCK_EXCL) excnt++;

          // Here's a corner case: If multiple adjacent shared holders
          // release the lock one after another, the queue entries may be
          // overwritten before the first holder locates all exclusive
          // waiters, causing it to wait infinitely. Hence, we return once
          // a overwritten entry is detected.
          if (entry->ver != BITMASK(VERSION_BITS) &&
              entry->ver > VERSION(head + i)) {
            remote_lock_reset(lock, tid, old_meta ASYNC_ARGS);
            return;
          }
        }
        LC_DEBUG("excnt %d, should be %ld, tried %d times", 
          excnt, EXCL_CNT(meta), loop_cnt);

        // If the collected exclusive waiter number is less than excnt,
        // there should be unfilled entries, so we scan the latest queue again.
        if (excnt != EXCL_CNT(meta)) {
          add_counter(TID_CORE(tid), CTR_LQ_FETCH);
          rdma_read(tid, lm_of_lock(lock), (char *)queue, LQE_ADDR(lock, 0),
            MAX_QUEUE_SIZE * sizeof(lqe), false);
          RDMA_WAIT_ONE(lm_of_lock(lock), tid);
          excnt = 0;
        } 

        loop_cnt++;
        digest_reset_notifications(lock, tid ASYNC_ARGS);
      }

      // If there are remaining shared holders, we do not need to notify
      // anyone. Otherwise, just notify the exclusive waiter.
      LC_DEBUG("first waiter: op %d, index %ld, mid %d, tid %d, ver %d",
        entry->op, head, entry->mid, entry->tid, entry->ver);
      if (entry->ver != VERSION(head) || entry->op == LOCK_SHARED) return;
      else NOTIFY_WAITERS(lock, tid, queue, head, 1, nrst);

    // If the exclusive holder releases the lock, 
    // all lock queue entries should be valid.
    } else {
      if (entry->ver != VERSION(head)) 
        FETCH_LQE(lock, tid, entry, head, old_meta);

      // If the first waiter is exclusive, or there is only one waiter left,
      // just transfer the lock to it.
      if (entry->op == LOCK_EXCL || LQ_SIZE(meta) == 2) {
        NOTIFY_WAITERS(lock, tid, queue, head, 1, nrst);

      // Otherwise, we might need to grant the lock 
      // to multiple shared waiters.
      } else {
        auto qsize = LQ_SIZE(meta) - 2;

        // Collect shared waiters from the fetched lock queue.
        // When meeting an invalid entry, retry fetching until valid.
        uint16_t wcnt;
        for (wcnt = 1; wcnt <= qsize; wcnt++) {
          entry = &queue[(head + wcnt) % MAX_QUEUE_SIZE];
          if (entry->ver != VERSION(head + wcnt))
            FETCH_LQE(lock, tid, entry, head + wcnt, old_meta);

          // Stop when meeting an exclusive waiter.
          if (entry->op == LOCK_EXCL) break;
        }
        
        // Notify all pending waiters to grant them the lock.
        NOTIFY_WAITERS(lock, tid, queue, head, wcnt, nrst);
      }
    } 
  }
}

/**
 * Fetch the lock header and the lock queue in one RDMA READ.
 */
void* remote_lock_fetch(uint32_t lock, uint32_t tid ASYNC_PARAMS) {
  auto core = TID_CORE(tid);
  auto buf = rdma_rc_buf(tid);
  auto lmeta = (void *)SENDBUF_LOCK_QUEUE(buf);
  rdma_read(tid, lm_of_lock(lock), (char *)lmeta, HDR_ADDR(lock),
    sizeof(aql), false);
  RDMA_WAIT_ONE(lm_of_lock(lock), tid);
  return lmeta;
}

/**
 * Check whether the remote lock is granted to a core.
 * 
 * Returns 1 if the lock is granted, 0 if the lock is aborted.
 * If USE_TIMESTAMP is enabled, returns 1 ^ (ts << 1) if
 * the lock is granted.
 */
int remote_wait_for_lock(uint32_t lock, uint32_t tid, char op ASYNC_PARAMS) {
  auto core = TID_CORE(tid);
  auto start_tp = steady_clock::now();
  while (1) {
    bool aborted = digest_reset_notifications(lock, tid ASYNC_ARGS);
    if (aborted) return 0;

#ifdef USE_COORDINATOR

    // Abort the acquisition and reset the lock if the acquisition timeouts.
    // To avoid too many concurrent resets, we add an cn-specific offset.
    auto wait_t = duration_cast<milliseconds>(steady_clock::now() - start_tp);
    if (wait_t.count() > ACQUIRE_TIMEOUT + LHID) {
      auto buf = (uint64_t *)rdma_rc_buf(tid);
      *buf = 0xdeadbeef;
      remote_lock_reset(lock, tid, buf ASYNC_ARGS);

      // After resetting the lock, try to acquire it again with 
      // refreshed timeout.
      if (remote_rw_lock(lock, tid, op ASYNC_ARGS)) return 1;
      start_tp = steady_clock::now();
    }
#endif

    // Try to find the notification from the local buffer first.
    auto ts = granted_locks[tid].get(lock);
    if (ts) {
      // delay_w_yield(2000 * 4 ASYNC_ARGS);
      LC_DEBUG("received lock grant signal");
      granted_locks[tid].erase(lock);
      return ts;
    }
    YIELD;

    // Poll notifications from the core's QP.
    wake_waiter_args arg = {.tid = tid};
    rdma_poll_recvs(core, wake_waiter, (void *)(&arg));
  }
}

int remote_check_resets(uint32_t lock, uint32_t tid ASYNC_PARAMS) {
  if (digest_reset_notifications(lock, tid ASYNC_ARGS)) return 1;

  auto core = TID_CORE(tid);
  wake_waiter_args arg = {.tid = tid};
  rdma_poll_recvs(core, wake_waiter, (void *)(&arg));
  return 0;
}

int remote_lock_reset(uint32_t lock, uint32_t tid, uint64_t* hdr ASYNC_PARAMS) {
  auto meta = *hdr;
  LC_DEBUG("reset lock");

  /**
   * Step 1: Set the reset ID.
   * 
   * This step is atomic (CAS), guaranteeing that only one client will execute
   * the reset procedure.
   */
  while (1) {
    rdma_cas(tid, lm_of_lock(lock), (char *)hdr, HDR_ADDR(lock), 
      meta, 1, false);
    RDMA_WAIT_ONE(lm_of_lock(lock), tid);
    LC_DEBUG("set the reset flag, return val 0x%lx, need 0x%lx", *hdr, meta);

    // If the reset flag has been set, terminate the reset process.
    if (RST_BIT(*hdr)) return 0;
    if (*hdr == meta) break;
    meta = *hdr;
  }

  // Increment the local reset count.
  timer(LOCK_RESET, TID_CORE(tid));
  auto nrst = rst_cnt.insertOrFind(lock);
  nrst.assignValue(nrst.getValue() + 1);
  LC_DEBUG("local rst cnt -> %d", nrst.getValue());
  add_counter(TID_CORE(tid), CTR_LOCK_RST);

  /**
   * Step 2: Notify participants.
   */
  bool sync;
  auto buf = (aql_notification *)rdma_sendbuf(tid,
                sizeof(aql_notification), &sync);
  buf->lock = lock;
  buf->rst = 1;
  buf->rst_cnt = nrst.getValue();

#ifdef USE_COORDINATOR
  uint32_t surviving_cns[MAX_CN_NUM];
  int surviving_cn_num = get_surviving_cns(surviving_cns);
#else
  uint32_t surviving_cns[MAX_CN_NUM];
  for (int i = 0; i < get_machine_num(); i++)
    surviving_cns[i] = i + MN_LIST.size();
  int surviving_cn_num = get_machine_num() - MN_LIST.size();
#endif

  // Notify other cores (local and remote).
  for (int i = 0; i < surviving_cn_num; i++) {
    auto mid = surviving_cns[i];
    for (int j = 0; j < CORE_NUM; j++) {
      if (mid == LHID && j == TID_CORE(tid)) continue;
      LC_DEBUG("sending notification to machine %d core %d", mid, j);
      rdma_send(TID_CORE(tid), mid, j, (char *)buf, 
        sizeof(aql_notification), sync);
    }
  }

  // Notify other coroutines on the same core.
  for (int i = 0; i < CRT_NUM; i++) {
    if (TID_CRT(tid) == i) continue;
    rst_locks[MAKE_TID(TID_CORE(tid), i)].insert(lock);
  }

  // Wait until all the other clients release the lock.
  auto desired = surviving_cn_num * CORE_NUM * CRT_NUM - 1;
  while (1) {
    rdma_read(tid, lm_of_lock(lock), (char *)hdr, HDR_ADDR(lock), 8, false);
    RDMA_WAIT_ONE(lm_of_lock(lock), tid);
    LC_DEBUG("check lock header, return %ld, need %d", 
      RST_CNT(*hdr), desired);
    
    // Avoid deadlock between resetting different locks.
    digest_reset_notifications(lock, tid ASYNC_ARGS);
    if (RST_CNT(*hdr) == desired) break;
  }

  /**
   * Step 3: Reset the lock object.
   * 
   * Reset the lock queue and header in sequence.
   */
  auto queue = (lqe *)SENDBUF_LOCK_QUEUE(rdma_rc_buf(tid));
  memset(queue, 0, sizeof(lqe) * MAX_QUEUE_SIZE);
  for (int i = 0; i < MAX_QUEUE_SIZE; i++) 
    queue[i].ver = BITMASK(VERSION_BITS);
  rdma_write(tid, lm_of_lock(lock), (char *)queue, 
    LQUEUE_ADDR(lock), sizeof(lqe) * MAX_QUEUE_SIZE, false);
  RDMA_WAIT_ONE(lm_of_lock(lock), tid);

  // Reset the lock header.
  meta = *hdr;
  while (1) {
    rdma_cas(tid, lm_of_lock(lock), (char *)hdr, HDR_ADDR(lock), 
      meta, 0, false);
    RDMA_WAIT_ONE(lm_of_lock(lock), tid);
    LC_DEBUG("reset header, return val 0x%lx, need 0x%lx", *hdr, meta);

    if (*hdr == meta) break;
    meta = *hdr;
  }

  return 0;
}
