
#ifndef __DMLOCK_COHORT_H
#define __DMLOCK_COHORT_H

#include "remote.h"
#include <mutex>
#include <list>
#include <unordered_map>
#include <cstdint>

using std::mutex;
using std::list;
using std::unordered_map;

#define TS_OFF 0

/**
 * Return code of cohort lock APIs.
 */
enum cohort_code {
  /* cohort_acquire */
  CHT_GRANTED,
  CHT_SHARED,
  CHT_SUSPENDED,
};

/**
 * Cohort lock states.
 */
enum cohort_lock_state {
  WAIT_COHORT_LOCK = 0,
  WAIT_REMOTE_LOCK,
  GRANTED,
  REACQUIRE_REMOTE_LOCK,
  WAIT_ABORTED_LOCK,
};

#define COHORT_FREE    0
#define COHORT_SHARED  1
#define COHORT_WWAIT   2
#define COHORT_EXCL    3
#define COHORT_PENDING 4

/**
 * Cohort lock policies.
 */
enum cohort_lock_policy {
  READER_PREFERRING,
  WRITER_PREFERRING,
};

typedef struct {
  uint16_t   client;
  bool     shared;
#ifdef USE_TIMESTAMP
  uint32_t ts;
#endif
  char*    state;
} cht_lqe;

struct cohort_lock {
  uint8_t       state;     /* FREE | SHARED | EXCL */
  uint16_t      grant_cnt; /* How many times has the lock been granted */

  uint8_t       sh_waiter_cnt;
  uint8_t       holder_cnt;
  list<cht_lqe> waiters;

  uint32_t      rwaiter_ts; /* Timestamp of the first remote waiter */

  cohort_mutex  mtx;

  cohort_lock(): state(COHORT_FREE), grant_cnt(0), 
                 holder_cnt(0), rwaiter_ts(0) {}
};

int cohort_acquire(uint32_t lid, uint16_t cid, bool shared, 
  char* granted ASYNC_PARAMS);
int cohort_release(uint32_t lid, uint16_t cid, bool shared ASYNC_PARAMS);
void cohort_after_grant(uint32_t lid, uint16_t cid, uint32_t remote_ret, 
  bool shared ASYNC_PARAMS);
int cohort_abort(uint32_t lid, uint16_t cid, bool shared, bool waiting ASYNC_PARAMS);
void cohort_set_grant_cnt(int n);

#endif