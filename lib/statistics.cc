
#include <chrono>
#include <vector>
#include <algorithm>
#include <unordered_map>

#include "statistics.h"
#include "sconf.h"
#include "util.h"
#include "conf.h"
#include "debug.h"
#include "lcore.h"

using std::unordered_map;
using std::vector;
using namespace std::chrono;
using namespace std::chrono_literals;

/******************************************************
 * Global utilities
 ******************************************************/
#define LKTSK(lid, tid) (((uint64_t)lid << 32) ^ (uint64_t)tid)

static steady_clock::time_point start_tp;

void start_profiler() {
  start_tp = steady_clock::now();
}

/******************************************************
 * Timer 
 ******************************************************/

static vector<uint64_t> latency_vec[TIMER_TYPE_MAX][MAX_CORE_NUM];

static const char* timer_type_str[] = {
  [LOCK_ACQUIRE] = "lock acquire",
  [LOCK_RELEASE] = "lock release",
  [LOCK_RESET] = "lock reset",
  [BRKDOWN_1] = "breakdown 1",
  [BRKDOWN_2] = "breakdown 2",
  [BRKDOWN_3] = "breakdown 3",
  [BRKDOWN_4] = "breakdown 4",
  [BRKDOWN_5] = "breakdown 5",
  [BRKDOWN_6] = "breakdown 6",
  [LOCK_ACQUIRE_ISSUE] = "lock acquire issue",
  [LOCK_ACQUIRE_GRANT] = "lock acquire grant",
  [RDMA_OP] = "rdma operation",
  [DATA_ACCESS] = "data access",
  [TXN] = "operation",
  "",
};

void record_latency(uint16_t core, timer_type t, uint64_t lat) {
  if (t < TIMER_TYPE_MAX) latency_vec[t][core].push_back(lat);
}

#define timer_merge(name) do {\
  for (int _core = 1; _core < MAX_CORE_NUM; _core++) {\
    name##_time[0].insert(\
      name##_time[_core].begin(), name##_time[_core].end());\
  }\
} while (0)

void report_timer(timer_type t) {
  uint32_t log_cnt = 0;
  for (int i = 0; i < latency_vec[t][0].size() && log_cnt < MAX_LOG_SIZE; i++) {
    for (int c = 0; c < MAX_CORE_NUM && log_cnt < MAX_LOG_SIZE; c++) {
      if (i < latency_vec[t][c].size()) {
        printf("%.2f\n", latency_vec[t][c][i] / 1000.0);
        log_cnt++;
      }
    }
  }
}

void report_timer_w_type(timer_type t) {
  uint32_t log_cnt = 0;
  for (int i = 0; log_cnt < MAX_LOG_SIZE; i++) {
    int finished = 0;
    for (int c = 0; c < MAX_CORE_NUM && log_cnt < MAX_LOG_SIZE; c++) {
      if (i < latency_vec[t][c].size()) {
        printf("%.2f,%d\n", latency_vec[t][c][i] / 1000.0, t);
        log_cnt++;
      } else finished++;
    }
    if (finished == MAX_CORE_NUM) break;
  }
}

void report_timer_percentage(timer_type t) {
  vector<uint64_t> lat;
  for (int c = 0; c < MAX_CORE_NUM; c++)
    lat.insert(lat.end(), latency_vec[t][c].begin(), latency_vec[t][c].end());
  if (lat.size() == 0) {
    return;
  }
  std::sort(lat.begin(), lat.end());
  auto size = lat.size();
  size_t p99 = (double)size * 0.99;
  fprintf(stderr, "%d, p50 %s latency %.2f\n", LHID, timer_type_str[t], 
    lat[size/2] / 1000.0);
  fprintf(stderr, "%d, p99 %s latency %.2f\n", LHID, timer_type_str[t], 
    lat[p99] / 1000.0);
}

/******************************************************
 * Counter 
 ******************************************************/

static const char* ctr_type_str[] = {
  "",
  [CTR_RDMA_READ] = "rdma read",
  [CTR_RDMA_WRITE] = "rdma write",
  [CTR_RDMA_FA] = "rdma fetch-and-add",
  [CTR_RDMA_CAS] = "rdma compare-and-swap",
  [CTR_RDMA_SEND] = "rdma send",
  [CTR_LQ_FETCH] = "lock queue fetch",
  [CTR_LOCK_RST] = "lock reset",
  [CTR_LOCAL_GRANT] = "lock local grant",
  "",
};

static uint64_t counters[MAX_CORE_NUM][CTR_TYPE_MAX];

uint64_t reset_counter(int core, int type) {
  counters[core][type] = 0;
}

uint64_t add_counter(int core, int type) {
  counters[core][type]++;
  return counters[core][type];
}

uint64_t get_counter(int core, int type) {
  return counters[core][type];
}

void report_counter() {
  double dur_s = latency(start_tp, steady_clock::now()) / 1000.0;

  // Report the counter value of each type.
  for (int type = CTR_BASE + 1; type < CTR_TYPE_MAX; type++) {
    uint64_t cnt = 0;
    for (int i = 0; i < MAX_CORE_NUM; i++) cnt += counters[i][type];
    
    if (type <= CTR_RDMA_SEND) {
      LLOG("[host%d]%s: throughput %.2f MIOPS", 
        LHID, ctr_type_str[type], cnt / dur_s);
    } else {
      LLOG("[host%d]%s: %ld times", 
        LHID, ctr_type_str[type], cnt);
    }
  }
}