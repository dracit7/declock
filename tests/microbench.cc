#include <vector>
#include <unordered_map>
#include <fstream>
#include <cstdint>
#include <thread>
#include <atomic>
#include <chrono>

#include <unistd.h>
#include <pthread.h>
#include <sched.h>

#include "async_adaptor.h"

#include "conf.h"
#include "lcore.h"
#include "sconf.h"
#include "util.h"
#include "random.h"
#include "debug.h"
#include "rdma.h"
#include "lm.h"
#include "lc.h"
#include "statistics.h"

#ifdef DMLOCK_SRC
#include "cohort.h"
#endif

using std::vector;
using std::unordered_map;
using std::ifstream;
using std::string;
using std::thread;
using namespace std::chrono;
using namespace std::chrono_literals;

/**
 * Microbench configurations.
 */
uint32_t LOCK_NUM = 100000;
uint32_t TEST_TIME_MS = 2000;

enum txn_wkld {
  TXN_SMALLBANK,
  TXN_TPCC,
};

#include "workload/smallbank.h"
#include "workload/tpcc.h"
#include "workload/micro.h"
#include "util/start_flag.hh"
#include "util/sighandler.hh"

static char* TRACE_FILE;
static int TRACE_SIZE = 0;
static int TXN_WORKLOAD;
static double ZIPF_ALPHA = 0.99;

/* MN mr layout. */
std::vector<uint64_t> line_num_to_key;
std::vector<uint64_t> value_addr; 
std::vector<uint64_t> value_size;
std::vector<std::uint8_t> op_array;
uint64_t MN_MR_SIZE;

static inline uint64_t next_multiple(uint64_t x, uint64_t base) {
  auto remainder = x % base;
  if (!remainder) return x;
  else return x + base - remainder;
}

/**
 * The workload type.
 */
enum workload {
  GENERATIVE,
  TRACE,
  TRANSACTION,
};
static enum workload req_workload;

#ifdef LEASELOCK_SRC
static constexpr uint64_t LEASE_DURATION_US = 50;
#endif

/**
 * Periodic throughput statistics.
 */
// #define PERIODIC_THPT
std::atomic<uint32_t> global_req_cnt;
uint32_t epoch_cnt;

/**
 * client_core: The client thread of CLM.
 *
 * `lcid`: The logic core id of current thread.
 * `cpu_id`: The cpu which current thread is bound to.
*/
void client_core(uint16_t lcid, uint16_t cpu_id) {
  // Set current lcore id.
  set_lcore_id(lcid);

  // Bind current thread to cpu core.
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

  LLOG("[host%u][core%u]Client core launched", LHID, get_cur_lcore_id());

  int rand_range;
  if (req_workload == TRANSACTION) {
    if (TXN_WORKLOAD == TXN_SMALLBANK) rand_range = SB_ACCOUNTS_NUM;
    if (TXN_WORKLOAD == TXN_TPCC) rand_range = TPCC_WAREHOUSE_NUM;
  } else rand_range = LOCK_NUM;

  auto local_lock_num = LOCK_NUM / CN_LIST.size();
  auto seed = duration_cast<nanoseconds>(
    steady_clock::now().time_since_epoch()).count();
  auto local_rand = Random(seed, 0, local_lock_num);
  auto global_rand = Random(seed, 0, rand_range);

  local_rand.init_zipfian(ZIPF_ALPHA); 
  global_rand.init_zipfian(ZIPF_ALPHA); 

  // Wait until the memory node sets the start flag.
  if (lcid == 0) cn_sync_time(FLAG_START_ADDR, MN_LIST[0], CN_LIST.size());
  else wait_until_start();
  reset_counter(lcid, CTR_RDMA_READ);
  LLOG("[host%u][core%u]microbench execution started, crt num %d", LHID, lcid,
    CRT_NUM);
  auto start_tp = steady_clock::now();
  int req_cnt = 0;
  int fin_cnt = 0;

  r2::SScheduler ssched;
  for (int crtid = 0; crtid < CRT_NUM; crtid++) {
    ssched.spawn([lcid, crtid, &local_rand, &global_rand, 
                  &req_cnt, &fin_cnt](R2_ASYNC) {

#ifdef DMLOCK_SRC
    uint32_t requester = MAKE_TID(lcid, crtid);
    uint32_t uid = CORE_NUM * CRT_NUM * (LHID - 1) + requester;
#else
    uint32_t requester = LHID * CORE_NUM * CRT_NUM + MAKE_TID(lcid, crtid);
    uint32_t uid = (LHID - 1) * CORE_NUM * CRT_NUM + MAKE_TID(lcid, crtid);
#endif

    // Generate and issue lock requests.
#ifdef PERIODIC_THPT
    auto thpt_tp = steady_clock::now();
    while (1) {
#else
    auto beg_tp = steady_clock::now();
    for (int i = 0; duration_cast<milliseconds>(steady_clock::now() - beg_tp)
      .count() < TEST_TIME_MS; i++) {
#endif
      timer(TXN, lcid);

      if (req_workload == TRANSACTION) {
        if (TXN_WORKLOAD == TXN_SMALLBANK) {
          sb_exec_txn(MAKE_TID(lcid, crtid), requester, global_rand ASYNC_ARGS);
        } else if (TXN_WORKLOAD == TXN_TPCC) {
          tpcc_exec_txn(MAKE_TID(lcid, crtid), requester, global_rand ASYNC_ARGS);
        }

      } else {
        uint32_t lid, op;
        uint64_t raddr, size;
        if (req_workload == TRACE) {
          auto line = (i * CORE_NUM * CRT_NUM * (MACHINES.size() - 1) + uid) 
                      % TRACE_SIZE;
          micro_exec_op(
            MAKE_TID(lcid, crtid), line_num_to_key[line], requester, 
            op_array[lid] > 1 ? LOCK_EXCL : LOCK_SHARED, 
            value_addr[lid] + MN_LOCK_MR_SIZE(LOCK_NUM), 
            value_size[lid] ASYNC_ARGS
          );

        } else {
          micro_gen_and_exec_op(MAKE_TID(lcid, crtid), requester, 
                global_rand, local_rand ASYNC_ARGS);
        }
      }
      req_cnt++;

#ifdef PERIODIC_THPT
      auto cnt = global_req_cnt.fetch_add(1);
      auto dur = duration_cast<microseconds>
                  (steady_clock::now() - thpt_tp).count();
      if (lcid == 0 && crtid == 0 && dur > 1000) {
        LLOG("[epoch%u][host%u]periodic throughput %.4f Mops", 
          epoch_cnt, LHID, cnt / (double)dur);
        global_req_cnt.store(0);
        thpt_tp = steady_clock::now();
        epoch_cnt++;
      }
#endif
    }

    fin_cnt++;
    if (fin_cnt == CRT_NUM) R2_STOP();
    R2_RET;
    });
  }

  ssched.run();

  // Calculate the lock granting throughput.
  auto end_tp = steady_clock::now();
  auto duration = duration_cast<microseconds>(end_tp - start_tp).count();
  double req_thpt = (double)req_cnt / (double)duration;
  LLOG("[host%u][core%u] request throughput %.4f Mops", LHID, lcid, req_thpt);

  // Calculate the RDMA op throughput.
  double fa_thpt = (double)get_counter(lcid, CTR_RDMA_FA) / duration;
  double read_thpt = (double)get_counter(lcid, CTR_RDMA_READ) / duration;
  double cas_thpt = (double)get_counter(lcid, CTR_RDMA_CAS) / duration;
  double write_thpt = (double)get_counter(lcid, CTR_RDMA_WRITE) / duration;
  LLOG("[host%u][core%u] rdma fa throughput %.4f", LHID, lcid, fa_thpt);
  LLOG("[host%u][core%u] rdma read throughput %.4f", LHID, lcid, read_thpt);
  LLOG("[host%u][core%u] rdma cas throughput %.4f", LHID, lcid, cas_thpt);
  LLOG("[host%u][core%u] rdma write throughput %.4f", LHID, lcid, write_thpt);

#ifdef DMLOCK_SRC
  double fetch_cnt = (double)get_counter(lcid, CTR_LQ_FETCH);
  double lgrt_cnt = (double)get_counter(lcid, CTR_LOCAL_GRANT);
  auto reset_cnt = get_counter(lcid, CTR_LOCK_RST);
  LLOG("[host%u][core%u] lqe fetch freq %.4f", LHID, lcid, fetch_cnt / req_cnt);
  LLOG("[host%u][core%u] local grant freq %.4f", LHID, lcid, lgrt_cnt / req_cnt);
  LLOG("[host%u][core%u] lock reset cnt %ld (req %d)", 
    LHID, lcid, reset_cnt, req_cnt);
#endif

}

/**
 * launch_cores: Launch the server cores and client cores.
 * Launching is numa-aware. If current process is running
 * on numa node 0, then the server cores and client cores
 * are launched to cpus with even number cpu id. Otherwise,
 * they are launched to cpus with odd number cpu id.
*/
void launch_cores() {
  vector<thread> threads;
  for (int i = 0; i < CORE_NUM; i++)
    threads.push_back(thread(client_core, i, 2 * i + NUMA_ID));
  for (int i = 0; i < CORE_NUM; i++)
    if (threads[i].joinable()) threads[i].join();
}

/**
 * read trace file to map key to value addr and get total_value_size.
 */
uint64_t trace_test_init(const string& trace_file) {
  LLOG("[host%u]trace test started", LHID);
  req_workload = TRACE;

  std::ifstream infile(trace_file);
  std::string line;
  int max_key = -1;
  uint64_t total_value_size = 0;

  while (std::getline(infile, line)) {
    std::stringstream ss(line);
    std::string key_str, value_size_str, op_str;

    if (std::getline(ss, key_str, ',') 
     && std::getline(ss, value_size_str, ',') 
     && std::getline(ss, op_str, ',')) {
      int key = std::stoi(key_str);
      line_num_to_key.push_back(key);

      // If the key is first met, update value_addr and value_size.
      if (key > max_key) {
        max_key = key;
        value_addr.push_back(total_value_size);
        value_size.push_back(std::stoi(value_size_str));
        total_value_size += value_size.back();
      }
      op_array.push_back(std::stoi(op_str));
    }

    TRACE_SIZE++;
  }

  LOCK_NUM = max_key + 1;
  return total_value_size + MN_LOCK_MR_SIZE(LOCK_NUM) + MN_FLAG_SIZE;
}

int main(int argc, char* argv[]) {
  get_conf_from_env();

  if (argc < 9) {
    ERROR("Usage: test_microbench "
      "[cn|mn] [zipf_alpha] [read_ratio] [exec_time] [locality] [cn_core_num] [grant_cnt] [data_size] [lock_num]");
  }

  // Parse all command line arguments.
  auto node_type = argv[1];
  auto access_dist = argv[2];

  auto local_grant_cnt = atoi(argv[6]);

  // Get request distribution configuration.
  if (!strcmp(access_dist, "trace")) {
    TRACE_FILE = argv[3];
    MN_MR_SIZE = next_multiple(trace_test_init(TRACE_FILE), 64);
    req_workload = TRACE;

  } else if (!strcmp(access_dist, "txn")) {
    uint64_t data_region_size = 0;
    if (!strcmp(argv[3], "smallbank")) {
      TXN_WORKLOAD = TXN_SMALLBANK;
      LOCK_NUM = SB_LOCK_NUM;
      data_region_size = SB_DATA_REGION_SIZE;
    } else if (!strcmp(argv[3], "tpcc")) {
      TXN_WORKLOAD = TXN_TPCC;
      LOCK_NUM = TPCC_LOCK_NUM;
      data_region_size = 0;
    } else ERROR("Invalid workload.");

    MN_MR_SIZE = next_multiple(MN_LOCK_MR_SIZE(LOCK_NUM) + 
      data_region_size + MN_FLAG_SIZE, 64);
    req_workload = TRANSACTION;

  } else {
    ZIPF_ALPHA = atof(argv[2]);
    LOCK_NUM = atoi(argv[8]);
    micro_config(ZIPFIAN, atoi(argv[3]), atoi(argv[4]), 
      atoi(argv[7]), atoi(argv[5]), LOCK_NUM);
    MN_MR_SIZE = next_multiple(MN_LOCK_MR_SIZE(LOCK_NUM) + 
      MICRO_DATA_REGION_SIZE + MN_FLAG_SIZE, 64);
    req_workload = GENERATIVE;
  }

  // Get the rest of arguments.
  if (!strcmp(node_type, "cn")) {

    // Set up RDMA QPs.
    bitmap core_map = 0;
    for (int i = 0; i < CORE_NUM; i++) BITMAP_SET(core_map, i);
    rdma_setup_cn(NUMA_ID, core_map);
    char* buf = rdma_sendbuf(0, 0, NULL);

#ifdef DMLOCK_SRC
    dmlock_setup_lc(LOCK_NUM);
    cohort_set_grant_cnt(local_grant_cnt);
#endif

#ifdef DSLR_SRC
    dslr_setup_lc(LOCK_NUM, core_map);
#endif

    setup_sighandler();
    launch_cores();

    cn_exit(LHID);
    report_timer_w_type(LOCK_ACQUIRE);
    report_timer_w_type(LOCK_RELEASE);
#ifdef DMLOCK_SRC
    report_timer_w_type(LOCK_RESET);
#endif
    report_timer_w_type(DATA_ACCESS);
    report_timer_w_type(LOCK_ACQUIRE_0);
    report_timer_w_type(TXN);
    report_timer_percentage(TXN);
    LLOG("[host%u] exited", LHID);

  } else {
    rdma_setup_mn(NUMA_ID, MN_MR_SIZE);
    LLOG("[host%u]mn is up, mr size 0x%lx", LHID, MN_MR_SIZE);

#if defined(DMLOCK_SRC)
    dmlock_setup_lm(LOCK_NUM, rdma_mn_buf());
#elif defined(DSLR_SRC)
    dslr_setup_lm(LOCK_NUM);
#elif defined(CASLOCK_SRC)
    caslock_setup_lm(LOCK_NUM);
#endif

    while (1) ;
  }

  return 0;
}
