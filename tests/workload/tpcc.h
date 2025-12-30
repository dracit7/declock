#include <algorithm>
#include <atomic>
#include <cstdint>
#include <fcntl.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "async_adaptor.h"
#include "debug.h"
#include "random.h"
#include "lc.h"
#include "lm.h"

#define TPCC_DISTRICT_PER_WAREHOUSE  10
#define TPCC_CUSTOMER_PER_DISTRICT   300
#define TPCC_ITEM_PER_WAREHOUSE      100000

#define TPCC_WAREHOUSE_NUM     8
#define TPCC_DISTRICT_NUM      (TPCC_WAREHOUSE_NUM * TPCC_DISTRICT_PER_WAREHOUSE)
#define TPCC_CUSTOMER_NUM      (TPCC_DISTRICT_NUM * TPCC_CUSTOMER_PER_DISTRICT)
#define TPCC_ORDER_NUM         (TPCC_CUSTOMER_NUM)
#define TPCC_HISTORY_NUM       (TPCC_CUSTOMER_NUM)
#define TPCC_NEWORDER_NUM      (TPCC_CUSTOMER_NUM)
#define TPCC_ORDERLINE_NUM     (TPCC_ORDER_NUM * 10)
#define TPCC_STOCK_NUM         (TPCC_WAREHOUSE_NUM * TPCC_ITEM_PER_WAREHOUSE)

#define TPCC_WH_LOCK_OFF      0
#define TPCC_DI_LOCK_OFF      TPCC_WAREHOUSE_NUM
#define TPCC_CU_LOCK_OFF      (TPCC_DI_LOCK_OFF + TPCC_DISTRICT_NUM)
#define TPCC_OD_LOCK_OFF      (TPCC_CU_LOCK_OFF + TPCC_CUSTOMER_NUM)
#define TPCC_HI_LOCK_OFF      (TPCC_OD_LOCK_OFF + TPCC_ORDER_NUM)
#define TPCC_NO_LOCK_OFF      (TPCC_HI_LOCK_OFF + TPCC_HISTORY_NUM)
#define TPCC_OL_LOCK_OFF      (TPCC_NO_LOCK_OFF + TPCC_NEWORDER_NUM)
#define TPCC_ST_LOCK_OFF      (TPCC_OL_LOCK_OFF + TPCC_ORDERLINE_NUM)
#define TPCC_LOCK_NUM         (TPCC_ST_LOCK_OFF + TPCC_STOCK_NUM)

#define TPCC_GLOB_DIST_ID(whid, distid) \
  ((whid) * TPCC_DISTRICT_PER_WAREHOUSE + (distid))
#define TPCC_GLOB_CUST_ID(whid, distid, custid) \
  (TPCC_GLOB_DIST_ID(whid, distid) * TPCC_CUSTOMER_PER_DISTRICT + (custid))
#define TPCC_GLOB_ORDERLINE_ID(whid, distid, olid) \
  (TPCC_GLOB_DIST_ID(whid, distid) * TPCC_CUSTOMER_PER_DISTRICT * 10 + (olid))
#define TPCC_GLOB_STOCK_ID(whid, stid) \
  ((whid) * TPCC_ITEM_PER_WAREHOUSE + (stid))

#define TPCC_WH_LOCK(whid)                 (whid)
#define TPCC_DI_LOCK(whid, distid)         (TPCC_DI_LOCK_OFF + TPCC_GLOB_DIST_ID(whid, distid))
#define TPCC_CU_LOCK(whid, distid, custid) (TPCC_CU_LOCK_OFF + TPCC_GLOB_CUST_ID(whid, distid, custid))
#define TPCC_OD_LOCK(whid, distid, odid)   (TPCC_OD_LOCK_OFF + TPCC_GLOB_CUST_ID(whid, distid, odid))
#define TPCC_HI_LOCK(whid, distid, hiid)   (TPCC_HI_LOCK_OFF + TPCC_GLOB_CUST_ID(whid, distid, hiid))
#define TPCC_NO_LOCK(whid, distid, noid)   (TPCC_NO_LOCK_OFF + TPCC_GLOB_CUST_ID(whid, distid, noid))
#define TPCC_OL_LOCK(whid, distid, olid)   (TPCC_OL_LOCK_OFF + TPCC_GLOB_ORDERLINE_ID(whid, distid, olid))
#define TPCC_ST_LOCK(whid, stid)           (TPCC_ST_LOCK_OFF + TPCC_GLOB_STOCK_ID(whid, stid))

#define URAND(range) (rand.next_uniform_long() % range)
#define NURAND(A, range) ((URAND(A) | (URAND(range))) % range)

static int ORDER_NUM[TPCC_DISTRICT_NUM];
static int NEWORDER_NUM[TPCC_DISTRICT_NUM];
static int ORDERLINE_NUM[TPCC_DISTRICT_NUM];
static int HISTORY_NUM[TPCC_DISTRICT_NUM];

#define ACQUIRE_LOCK_SYNC(lock, op, tid, locks_vec) do {\
  locks_vec.push_back(std::make_pair(lock, op));\
  auto _l = acquire_lock(lock, op, tid);\
  if (!_l) wait_for_lock(lock, op, tid);\
} while (0)

// printf("%d,0,2,%d,%d\n", txnid, lock, op == LOCK_SHARED ? 1 : 2);\

static std::atomic_int tpcc_txn_count;

inline int tpcc_neworder(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = tpcc_txn_count.fetch_add(1);
  std::vector<std::pair<uint32_t, int>> locks;

  int whid = rand.next_zipfian();
  int distid = URAND(TPCC_DISTRICT_PER_WAREHOUSE);
  int custid = NURAND(1024, TPCC_CUSTOMER_PER_DISTRICT);

  ACQUIRE_LOCK_SYNC(TPCC_WH_LOCK(whid), LOCK_SHARED, requester, locks);
  ACQUIRE_LOCK_SYNC(TPCC_DI_LOCK(whid, distid), LOCK_EXCL, requester, locks);
  ACQUIRE_LOCK_SYNC(TPCC_CU_LOCK(whid, distid, custid), LOCK_EXCL, requester, locks);

  auto glob_distid = TPCC_GLOB_DIST_ID(whid, distid);
  auto odid = (ORDER_NUM[glob_distid]++) % TPCC_CUSTOMER_PER_DISTRICT;
  auto noid = (NEWORDER_NUM[glob_distid]++) % TPCC_CUSTOMER_PER_DISTRICT;

  ACQUIRE_LOCK_SYNC(TPCC_OD_LOCK(whid, distid, odid), LOCK_EXCL, requester, locks);
  ACQUIRE_LOCK_SYNC(TPCC_NO_LOCK(whid, distid, noid), LOCK_EXCL, requester, locks);

  auto item_num = URAND(10) + 5;
  std::vector<int> items;
  for (int i = 0; i < item_num; i++) {
    auto whid2 = (URAND(100) == 0) ? URAND(TPCC_WAREHOUSE_NUM) : whid;
    auto stid = NURAND(8192, TPCC_ITEM_PER_WAREHOUSE);
    items.push_back(TPCC_ST_LOCK(whid, stid));
  }

  std::sort(items.begin(), items.end());
  auto unique_it = std::unique(items.begin(), items.end());
  items.erase(unique_it, items.end());

  for (auto const& stock : items)
    ACQUIRE_LOCK_SYNC(stock, LOCK_EXCL, requester, locks);

  auto olid = (ORDERLINE_NUM[glob_distid]++) % (TPCC_CUSTOMER_PER_DISTRICT * 10);
  ACQUIRE_LOCK_SYNC(TPCC_OL_LOCK(whid, distid, olid), LOCK_EXCL, requester, locks);

  for (auto lock = locks.rbegin(); lock != locks.rend(); lock++) {
    // LLOG("tid %d release lock %d (op %d)", requester, lock->first, lock->second);
    release_lock(lock->first, lock->second, requester);
  }
  return 0;
}

inline int tpcc_payment(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = tpcc_txn_count.fetch_add(1);
  std::vector<std::pair<uint32_t, int>> locks;

  int whid = rand.next_zipfian();
  int distid = URAND(TPCC_DISTRICT_PER_WAREHOUSE);
  auto glob_distid = TPCC_GLOB_DIST_ID(whid, distid);

  ACQUIRE_LOCK_SYNC(TPCC_WH_LOCK(whid), LOCK_EXCL, requester, locks);
  ACQUIRE_LOCK_SYNC(TPCC_DI_LOCK(whid, distid), LOCK_EXCL, requester, locks);
  
  int custid = (URAND(100) < 60) ? 
              (NURAND(256, 1000) % TPCC_CUSTOMER_PER_DISTRICT) :
              (NURAND(1024, 3000) % TPCC_CUSTOMER_PER_DISTRICT);

  if (URAND(100) < 85) {
    ACQUIRE_LOCK_SYNC(TPCC_CU_LOCK(whid, distid, custid), 
                LOCK_EXCL, requester, locks);
  } else {
    int rwhid = URAND(TPCC_WAREHOUSE_NUM);
    ACQUIRE_LOCK_SYNC(TPCC_CU_LOCK(rwhid, distid, custid), 
                LOCK_EXCL, requester, locks);
  }

  auto hiid = (HISTORY_NUM[glob_distid]++) % TPCC_CUSTOMER_NUM;
  ACQUIRE_LOCK_SYNC(TPCC_HI_LOCK(whid, distid, hiid), LOCK_EXCL, requester, locks);

  for (auto lock = locks.rbegin(); lock != locks.rend(); lock++) {
    release_lock(lock->first, lock->second, requester);
  }
}

inline int tpcc_orderstatus(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = tpcc_txn_count.fetch_add(1);
  std::vector<std::pair<uint32_t, int>> locks;

  int whid = rand.next_zipfian();
  int distid = URAND(TPCC_DISTRICT_PER_WAREHOUSE);
  int custid = (URAND(100) < 60) ? 
              (NURAND(256, 1000) % TPCC_CUSTOMER_PER_DISTRICT) :
              (NURAND(1024, 3000) % TPCC_CUSTOMER_PER_DISTRICT);

  ACQUIRE_LOCK_SYNC(TPCC_CU_LOCK(whid, distid, custid), 
              LOCK_SHARED, requester, locks);
  
  int odid = URAND(TPCC_CUSTOMER_PER_DISTRICT);
  int olid = URAND(TPCC_CUSTOMER_PER_DISTRICT * 10);

  ACQUIRE_LOCK_SYNC(TPCC_OD_LOCK(whid, distid, odid), LOCK_SHARED, requester, locks);
  ACQUIRE_LOCK_SYNC(TPCC_OL_LOCK(whid, distid, olid), LOCK_SHARED, requester, locks);

  for (auto lock = locks.rbegin(); lock != locks.rend(); lock++) {
    release_lock(lock->first, lock->second, requester);
  }
}

inline int tpcc_delivery(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = tpcc_txn_count.fetch_add(1);
  std::vector<std::pair<uint32_t, int>> locks;

  int whid = rand.next_zipfian();
  int distid = URAND(TPCC_DISTRICT_PER_WAREHOUSE);

  int odid = URAND(TPCC_CUSTOMER_PER_DISTRICT);
  int noid = URAND(TPCC_CUSTOMER_PER_DISTRICT);
  int olid = URAND(TPCC_CUSTOMER_PER_DISTRICT * 10);

  ACQUIRE_LOCK_SYNC(TPCC_OD_LOCK(whid, distid, odid), LOCK_EXCL, requester, locks);
  ACQUIRE_LOCK_SYNC(TPCC_NO_LOCK(whid, distid, noid), LOCK_EXCL, requester, locks);
  ACQUIRE_LOCK_SYNC(TPCC_OL_LOCK(whid, distid, olid), LOCK_SHARED, requester, locks);

  int custid = NURAND(1024, 3000) % TPCC_CUSTOMER_PER_DISTRICT;
  ACQUIRE_LOCK_SYNC(TPCC_CU_LOCK(whid, distid, custid), 
              LOCK_EXCL, requester, locks);

  for (auto lock = locks.rbegin(); lock != locks.rend(); lock++) {
    release_lock(lock->first, lock->second, requester);
  }
}

inline int tpcc_stocklevel(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  auto txnid = tpcc_txn_count.fetch_add(1);
  std::vector<std::pair<uint32_t, int>> locks;

  int whid = rand.next_zipfian();
  int distid = URAND(TPCC_DISTRICT_PER_WAREHOUSE);

  ACQUIRE_LOCK_SYNC(TPCC_DI_LOCK(whid, distid), LOCK_SHARED, requester, locks);

  int odid = URAND(TPCC_CUSTOMER_PER_DISTRICT);
  int stid = URAND(TPCC_ITEM_PER_WAREHOUSE);

  ACQUIRE_LOCK_SYNC(TPCC_OD_LOCK(whid, distid, odid), LOCK_SHARED, requester, locks);
  ACQUIRE_LOCK_SYNC(TPCC_ST_LOCK(whid, stid), LOCK_SHARED, requester, locks);

  for (auto lock = locks.rbegin(); lock != locks.rend(); lock++) {
    release_lock(lock->first, lock->second, requester);
  }
}

inline int tpcc_exec_txn(uint32_t tid, uint32_t requester, Random& rand ASYNC_PARAMS) {
  int k = rand.next_uniform_long() % 100;
  // int k = 40;
  if (k <= 45) {
    return tpcc_neworder(tid, requester, rand ASYNC_ARGS);
  } else if (k <= 88) {
    return tpcc_payment(tid, requester, rand ASYNC_ARGS);
  } else if (k <= 92) {
    return tpcc_orderstatus(tid, requester, rand ASYNC_ARGS);
  } else if (k <= 96) {
    return tpcc_delivery(tid, requester, rand ASYNC_ARGS);
  } else {
    return tpcc_stocklevel(tid, requester, rand ASYNC_ARGS);
  }
}