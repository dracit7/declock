#include "lm.h"
#include "rdma.h"
#include "lcore.h"
#include "debug.h"
#include "conf.h"

#define RDMA_WAIT_ONE(mn, tid) \
  do { YIELD; } while (!rdma_poll_one(mn, tid))

lock_obj_t fa_lock_obj_generic(uint16_t dest, uint64_t raddr, lock_obj_t add_val ASYNC_PARAMS) {
  uint32_t tid = MAKE_TID(get_cur_lcore_id(), R2_COR_ID() - 1);
  auto old_l = (lock_obj_t*)rdma_rc_buf(tid);
  rdma_fa(tid, dest, (char*)old_l, raddr, add_val, false);
  RDMA_WAIT_ONE(dest, tid);
  return *old_l;
}

lock_obj_t read_lock_obj(uint16_t dest, uint64_t raddr ASYNC_PARAMS) {
  uint32_t tid = MAKE_TID(get_cur_lcore_id(), R2_COR_ID() - 1);
  auto val = (lock_obj_t*)rdma_rc_buf(tid);
  rdma_read(tid, dest, (char*)val, raddr, sizeof(lock_obj_t), false);
  RDMA_WAIT_ONE(dest, tid);
  return *val;
}

bool cas_lock_obj(uint16_t dest, uint64_t raddr, lock_obj_t expected, lock_obj_t swap ASYNC_PARAMS) {
  uint32_t tid = MAKE_TID(get_cur_lcore_id(), R2_COR_ID() - 1);
  auto origin_val = (lock_obj_t*)rdma_rc_buf(tid);
  rdma_cas(tid, dest, (char*)origin_val, raddr, expected, swap, false);
  RDMA_WAIT_ONE(dest, tid);
  return *origin_val == expected;
}

bool cas_lock_obj(uint16_t dest, uint64_t raddr, lock_obj_t expected, lock_obj_t swap, lock_obj_t& actual ASYNC_PARAMS) {
  uint32_t tid = MAKE_TID(get_cur_lcore_id(), R2_COR_ID() - 1);
  auto origin_val = (lock_obj_t*)rdma_rc_buf(tid);
  rdma_cas(tid, dest, (char*)origin_val, raddr, expected, swap, false);
  RDMA_WAIT_ONE(dest, tid);
  actual = *origin_val;
  return *origin_val == expected;
}

int caslock_setup_lm(int lock_num) {
  auto mn_buf = rdma_mn_buf();

  // Initialize lock objects.
  auto* lock_objs = (lock_obj_t*)mn_buf;
  for (int i = 0; i < lock_num; i++) {
    lock_objs[i] = 0;
  }

  LLOG("[host%u]caslock set up lock manager, lock_num %d", LHID, lock_num);

  return 0;
}