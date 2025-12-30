
#include <atomic>
#include <cstdint>

#include "async_adaptor.h"
#include "conf.h"
#include "debug.h"
#include "lm.h"
#include "random.h"
#include "lc.h"
#include "rdma.h"
#include "util.h"

#define SB_ACCOUNTS_NUM 100000
#define SB_LOCK_NUM     (SB_ACCOUNTS_NUM * 2)

#define SB_ROW_SIZE         8
#define SB_DATA_REGION_SIZE (SB_ROW_SIZE * SB_ACCOUNTS_NUM * 2)

#define SB_CHECKING_TABLE_ROW(id)  \
  (MN_LOCK_MR_SIZE(SB_LOCK_NUM) + SB_ROW_SIZE * (id))
#define SB_SAVING_TABLE_ROW(id)  \
  (MN_LOCK_MR_SIZE(SB_LOCK_NUM) + SB_ROW_SIZE * SB_ACCOUNTS_NUM + SB_ROW_SIZE * (id))

#define SB_CHECKING_LID(id)  (id)
#define SB_SAVING_LID(id)    ((id) + SB_ACCOUNTS_NUM)

#define RDMA_WAIT_ONE(mn, tid) \
  do { YIELD; } while (!rdma_poll_one(mn, tid))

#define SB_ACQUIRE_LOCK_SYNC(lock, op, tid) do {\
  auto _l = acquire_lock(lock, op, tid);\
  if (!_l) wait_for_lock(lock, op, tid);\
} while (0)

// printf("%d,0,2,%ld,%d\n", txnid, lock, op == LOCK_SHARED ? 1 : 2);\

inline void sb_gen_account(Random& rand, uint64_t& a1) {
  a1 = rand.next_zipfian();
}

inline void sb_gen_accounts(Random& rand, uint64_t& a1, uint64_t& a2) {
  a1 = rand.next_zipfian();
  a2 = rand.next_zipfian();
  while (a2 == a1) a2 = rand.next_zipfian();
  if (a2 < a1) {
    auto mid = a2;
    a2 = a1;
    a1 = mid;
  }
}

static std::atomic_int sb_txn_count;

inline int sb_amalgamate(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = sb_txn_count.fetch_add(1);
  uint64_t a1, a2;
  sb_gen_accounts(rand, a1, a2);
  SB_ACQUIRE_LOCK_SYNC(SB_SAVING_LID(a1), LOCK_EXCL, requester);
  SB_ACQUIRE_LOCK_SYNC(SB_CHECKING_LID(a1), LOCK_EXCL, requester);
  SB_ACQUIRE_LOCK_SYNC(SB_CHECKING_LID(a2), LOCK_EXCL, requester);
  release_lock(SB_CHECKING_LID(a2), LOCK_EXCL, requester);
  release_lock(SB_CHECKING_LID(a1), LOCK_EXCL, requester);
  release_lock(SB_SAVING_LID(a1), LOCK_EXCL, requester);
  return 0;
}

inline int sb_balance(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = sb_txn_count.fetch_add(1);
  uint64_t a1;
  sb_gen_account(rand, a1);
  SB_ACQUIRE_LOCK_SYNC(SB_SAVING_LID(a1), LOCK_SHARED, requester);
  SB_ACQUIRE_LOCK_SYNC(SB_CHECKING_LID(a1), LOCK_SHARED, requester);
  release_lock(SB_CHECKING_LID(a1), LOCK_SHARED, requester);
  release_lock(SB_SAVING_LID(a1), LOCK_SHARED, requester);
  return 0;
}

inline int sb_deposit(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = sb_txn_count.fetch_add(1);
  uint64_t a1;
  sb_gen_account(rand, a1);
  SB_ACQUIRE_LOCK_SYNC(SB_CHECKING_LID(a1), LOCK_EXCL, requester);
  release_lock(SB_CHECKING_LID(a1), LOCK_EXCL, requester);
  return 0;
}

inline int sb_sendpayment(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = sb_txn_count.fetch_add(1);
  uint64_t a1, a2;
  sb_gen_accounts(rand, a1, a2);
  SB_ACQUIRE_LOCK_SYNC(SB_CHECKING_LID(a1), LOCK_EXCL, requester);
  SB_ACQUIRE_LOCK_SYNC(SB_CHECKING_LID(a2), LOCK_EXCL, requester);
  release_lock(SB_CHECKING_LID(a2), LOCK_EXCL, requester);
  release_lock(SB_CHECKING_LID(a1), LOCK_EXCL, requester);
  return 0;
}

inline int sb_txnsaving(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = sb_txn_count.fetch_add(1);
  uint64_t a1;
  sb_gen_account(rand, a1);
  SB_ACQUIRE_LOCK_SYNC(SB_SAVING_LID(a1), LOCK_EXCL, requester);
  release_lock(SB_SAVING_LID(a1), LOCK_EXCL, requester);
  return 0;
}

inline int sb_writecheck(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = sb_txn_count.fetch_add(1);
  uint64_t a1;
  sb_gen_account(rand, a1);
  SB_ACQUIRE_LOCK_SYNC(SB_SAVING_LID(a1), LOCK_SHARED, requester);
  SB_ACQUIRE_LOCK_SYNC(SB_CHECKING_LID(a1), LOCK_EXCL, requester);
  release_lock(SB_CHECKING_LID(a1), LOCK_EXCL, requester);
  release_lock(SB_SAVING_LID(a1), LOCK_SHARED, requester);
  return 0;
}

inline int sb_exec_txn(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  int k = rand.next_uniform_long() % 100;
  // int k = 80;
  if (k <= 25) {
    return sb_sendpayment(tid, requester, rand ASYNC_ARGS);
  } else if (k <= 40) {
    return sb_amalgamate(tid, requester, rand ASYNC_ARGS);
  } else if (k <= 55) {
    return sb_balance(tid, requester, rand ASYNC_ARGS);
  } else if (k <= 70) {
    return sb_deposit(tid, requester, rand ASYNC_ARGS);
  } else if (k <= 85) {
    return sb_txnsaving(tid, requester, rand ASYNC_ARGS);
  } else {
    return sb_writecheck(tid, requester, rand ASYNC_ARGS);
  }
}