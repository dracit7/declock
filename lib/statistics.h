#ifndef __DMLOCK_STATISTICS_H
#define __DMLOCK_STATISTICS_H

#include <stdint.h>
#include <chrono>

#define MAX_LOG_SIZE 100000

/**
 * The statistics module.
 * 
 * This module collects runtime statistics of the system,
 * such as latency.
 */

void start_profiler();

/**
 * Timer API.
 */
typedef enum {
  LOCK_ACQUIRE = 0,
  LOCK_RELEASE,
  LOCK_RESET,

  BRKDOWN_1,
  BRKDOWN_2,
  BRKDOWN_3,
  BRKDOWN_4,
  BRKDOWN_5,
  BRKDOWN_6,

  LOCK_ACQUIRE_ISSUE,
  LOCK_ACQUIRE_GRANT,

  RDMA_OP,
  DATA_ACCESS,
  TXN,

  LOCK_RESET_S1,
  LOCK_RESET_S2,
  LOCK_RESET_S3,

  LOCK_ACQUIRE_0,

  TIMER_TYPE_MAX,
} timer_type;

using std::chrono::duration_cast;
using std::chrono::nanoseconds;
using std::chrono::steady_clock;

#define latency(beg, fin) duration_cast<nanoseconds>(fin - beg).count()
void record_latency(uint16_t core, timer_type t, uint64_t lat);
void report_timer(timer_type t);
void report_timer_w_type(timer_type t);
void report_timer_percentage(timer_type t);

/** 
 * The class for recording the latency of a code block.
 * 
 * Usage:
 * {
 *   timer(LOCK_ACQUIRE, core);
 *   ... // Lock acquire logic
 * }
 */
#define timer(name, core) timer_class tm_##name(name, core)

struct timer_class {
  steady_clock::time_point start;
  uint16_t core;
  timer_type name;

public:
  timer_class(timer_type t, uint16_t c) {
    name = t;
    core = c;
    start = steady_clock::now();
  }

  ~timer_class() {
    record_latency(core, name, latency(start, steady_clock::now()));
  }
};

/**
 * Counter API.
 */

enum counter_type {
  CTR_BASE = 0,

  /* RDMA verbs */
  CTR_RDMA_READ,
  CTR_RDMA_WRITE,
  CTR_RDMA_FA,
  CTR_RDMA_CAS,
  CTR_RDMA_SEND,

  CTR_LQ_FETCH,
  CTR_LOCK_RST,
  CTR_LOCAL_GRANT,

  CTR_LOCK_OPS,
  CTR_LOCK_RETRY_OPS,

  CTR_TYPE_MAX,
};

uint64_t add_counter(int core, int type);
uint64_t get_counter(int core, int type);
uint64_t reset_counter(int core, int type);
void report_counter();

#endif