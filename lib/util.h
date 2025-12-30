
#ifndef __DMLOCK_UTIL_H
#define __DMLOCK_UTIL_H

#include <cstdint>
#include "conf.h"

/* A simple bitmap abstraction. */
typedef uint64_t bitmap;
#define BITMAP_CONTAIN(map, i) (((map) & (1 << (i))) != 0)
#define BITMAP_SET(map, i) (map) |= 1 << (i)

/* Task ID abstraction. */
#define MAKE_TID(core, crt) (((core) << 3) ^ (crt))
#define TID_CORE(tid) ((tid) >> 3)
#define TID_CRT(tid) ((tid) & 0x7)

/* A more accurate version of usleep(). */
void delay(uint64_t nanosec);

/* Timing utility. */
void set_start_timestamp();
void wait_until_start();
uint32_t timestamp_now(uint16_t core);

/* Return the result of (ts1 > ts2) */
static inline bool ts_later(uint32_t ts1, uint32_t ts2, uint32_t mask) {
  int range = mask + 1;
  if ((int)(ts1 - ts2) > range * 0.9) return false;
  if ((int)(ts2 - ts1) > range * 0.9) return true;
  return ts1 > ts2;
}

/* Distribute locks among MNs. */
static uint16_t lm_of_lock(uint32_t lock) {
  return MN_LIST[lock % MN_LIST.size()];
}

/* RPC specs for failure recovery. */
enum rpc_op {
  RPC_SERVER_JOIN = 128,
  RPC_REPORT_SERVER_FAILURE,
  RPC_GET_SURVIVING_SERVERS,
};

struct __attribute__((packed)) sid_req {
  unsigned server_id;
};

struct __attribute__((packed)) servers_reply {
  unsigned server_num;
  unsigned server_ids[MAX_CN_NUM];
};

void connect_coordinator();
void cn_join(uint32_t mid);
void cn_exit(uint32_t mid);
int get_surviving_cns(uint32_t* cns);

#endif