
#include <cstdint>
#include <chrono>

#include "async_adaptor.h"
#include "debug.h"
#include "lm.h"
#include "rdma.h"
#include "random.h"
#include "lc.h"
#include "conf.h"
#include "statistics.h"
#include "util.h"

using namespace std::chrono;
using namespace std::chrono_literals;

#define MICRO_DATA_REGION_SIZE (LOCK_NUM * MICROBENCH_DATA_SIZE)

enum distribution {
  UNIFORM,
  ZIPFIAN,
};

static int MICROBENCH_REQ_DISTRIBUTION = ZIPFIAN;
static int MICROBENCH_READ_RATIO;
static int MICROBENCH_SHOT_NUM = 1;
static int MICROBENCH_DATA_SIZE = 8;
static int MICROBENCH_LOCALITY = 0;
static int MICROBENCH_LOCK_NUM = 100000;

#define MICRO_LOCK_DATA_RADDR(lock_id) \
  (MN_LOCK_MR_SIZE(MICROBENCH_LOCK_NUM) + lock_id * MICROBENCH_DATA_SIZE)

#define RDMA_WAIT_ONE(mn, tid) \
  do { YIELD; } while (!rdma_poll_one(mn, tid))

inline int micro_exec_op(uint32_t tid, uint32_t lid, uint32_t requester, uint32_t op, 
  uint64_t raddr, uint64_t size ASYNC_PARAMS) {

  // Acquire lock.
  {
    auto start = steady_clock::now();
    timer(LOCK_ACQUIRE, TID_CORE(tid));
    if (!acquire_lock(lid, op, requester)) {
      if (!wait_for_lock(lid, op, requester)) { /* Aborted */
        return 1; 
      }
    }
    if (lid == 0) record_latency(TID_CORE(tid), LOCK_ACQUIRE_0, 
                        latency(start, steady_clock::now()));
  }

  // Here, MICROBENCH_SHOT_NUM denotes the number of batches
  // that should be issued.
  auto buf = rdma_rc_buf(tid);

  int dest = lm_of_lock(lid);
  {
    timer(DATA_ACCESS, TID_CORE(tid));
    for (int i = 0; i < MICROBENCH_SHOT_NUM; i++) {
    if (op == LOCK_SHARED) {
      rdma_read(tid, dest, buf, raddr, size, false);
    } else {
      rdma_write(tid, dest, buf, raddr, size, false);
    }
    RDMA_WAIT_ONE(dest, tid);
    }
  }

  // Release lock.
  {
    timer(LOCK_RELEASE, TID_CORE(tid));
    release_lock(lid, op, requester);
  }
  return 0;
}

inline int micro_gen_and_exec_op(uint32_t tid, uint32_t requester, Random& rand, Random& local_rand ASYNC_PARAMS) {
  uint32_t lid, op;
  uint64_t raddr, size;

  bool local = rand.next_uniform_long() % 100 < MICROBENCH_LOCALITY;
  if (local) {
    lid = (MICROBENCH_REQ_DISTRIBUTION == UNIFORM) ? 
            local_rand.next_uniform() : local_rand.next_zipfian();
    lid = lid * CN_LIST.size() + LHID - MN_LIST.size();
  } else {
    lid = (MICROBENCH_REQ_DISTRIBUTION == UNIFORM) ? 
            rand.next_uniform() : rand.next_zipfian();
  }

  op = (rand.next_uniform_long() % 100 < MICROBENCH_READ_RATIO) ? 
      LOCK_SHARED : LOCK_EXCL;
  raddr = MICRO_LOCK_DATA_RADDR(lid);
  size = MICROBENCH_DATA_SIZE;

  return micro_exec_op(tid, lid, requester, op, raddr, size ASYNC_ARGS);
}

inline void micro_config(int distribution, int read_ratio, int shot_num, int data_size, 
                  int locality, int lock_num) {
  MICROBENCH_REQ_DISTRIBUTION = distribution;
  MICROBENCH_READ_RATIO = read_ratio;
  MICROBENCH_SHOT_NUM = shot_num;
  MICROBENCH_DATA_SIZE = data_size;
  MICROBENCH_LOCALITY = locality;
  MICROBENCH_LOCK_NUM = lock_num;
}