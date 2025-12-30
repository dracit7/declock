#include <unordered_map>
#include <unordered_set>
#include <junction/ConcurrentMap_Leapfrog.h>
#include <cstdint>

#include "DSM.h"
#include "Rdma.h"

#include "lm.h"
#include "statistics.h"
#include "conf.h"
#include "rdma.h"
#include "lcore.h"

using std::unordered_map;
using std::unordered_set;
using namespace std::chrono;

#define NO_TS 0x1
#define TS_NULL 0x0
#define TS(ts) (1 ^ (ts << 1))

// Lock granting.
unordered_map<uint64_t, uint32_t> 
  sherman_granted_locks[MAX_CORE_NUM * SHERMAN_MAX_CRT_CNT];

#define LQUEUE_OFF(addr) (addr + sizeof(lmeta))
#define LQE_OFF(addr, slot) (LQUEUE_OFF(addr) + (slot) * sizeof(lqe))

#define SHERMAN_VER(i) (VERSION(i))

typedef struct {
  DSM* dsm;
  GlobalAddress lock_addr;
  uint32_t core_id;
  ibv_qp* qp;
  uint64_t* hdr_buf;
} wake_waiter_args;

#define DBG(fmt, ...) (0) // do {\
  fprintf(stdout, "[lock0x%llx][host%d][core%d]" fmt "\n", \
    lock_addr.offset, LHID, get_cur_lcore_id(), ##__VA_ARGS__);\
  fflush(stdout);\
} while (0)

int DSM::sherman_cql_acquire(GlobalAddress lock_addr, uint32_t core_id, 
  uint64_t *hdr_buf, CoroContext* ctx) {

  // timer(LOCK_CQL_ACQUIRE_ISSUE, get_cur_lcore_id());
  uint32_t ts = timestamp_now(core_id) & TIMESTAMP_BITMASK;

  // Enqueue the acquirer.
  {
  // timer(LOCK_CQL_ENQUEUE, get_cur_lcore_id());
  ibv_qp* qp = iCon->data[0][lock_addr.nodeID];
  uint64_t source = (uint64_t)hdr_buf;
  uint64_t dest = remoteInfo[lock_addr.nodeID].lockBase + lock_addr.offset;
  uint64_t add = LMETA(1, 0, 1);
  uint32_t lkey = iCon->cacheLKey;
  uint32_t remoteRKey = remoteInfo[lock_addr.nodeID].lockRKey[0];
  if (ctx == nullptr) {
    rdmaFetchAndAdd(qp, source, dest, add, lkey, remoteRKey);
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  } else {
    rdmaFetchAndAdd(qp, source, dest, add, lkey, remoteRKey, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
  }

  // Check granted.
  auto meta = *(uint64_t *)hdr_buf;
  DBG("acquire result: head %d size %d excl %d", LQ_HEAD(meta), 
    LQ_SIZE(meta), EXCL_CNT(meta));

  // Is the lock directly granted?
  if (LQ_SIZE(meta) == 0) {
    DBG("lock directly granted");
    return 1;

  // If not, populate the queue entry.
  } else {
    DBG("lock not directly granted");
    auto idx = LQ_HEAD(meta) + LQ_SIZE(meta);
    auto entry = (lqe *)hdr_buf;
    entry->op = 1;
    entry->mid = LHID;
    entry->tid = MAKE_TID(core_id, (ctx == nullptr) ? 0 : ctx->coro_id);
    entry->ver = SHERMAN_VER(idx);
    entry->ts = ts;

    rdmaWrite(iCon->data[0][lock_addr.nodeID], 
      (uint64_t)hdr_buf, remoteInfo[lock_addr.nodeID].lockBase + 
      LQE_OFF(lock_addr.offset, idx % MAX_QUEUE_SIZE),
      sizeof(lqe), iCon->cacheLKey, remoteInfo[lock_addr.nodeID].lockRKey[0],
      -1, false, 0);

    DBG("populated the queue entry %d, mid %u tid %u version %u", 
      idx, entry->mid, entry->tid, entry->ver);
    return 0;
  }
}

int DSM::sherman_cql_release(GlobalAddress lock_addr, uint32_t core_id, 
  uint64_t *hdr_buf, CoroContext* ctx) {

  // timer(LOCK_CQL_RELEASE, get_cur_lcore_id());

  {
  // timer(LOCK_CQL_RELEASE_ISSUE, get_cur_lcore_id());
  // Dequeue the acquirer.
  RdmaOpRegion faa_ror;
  faa_ror.source = (uint64_t)hdr_buf;
  faa_ror.dest = remoteInfo[lock_addr.nodeID].lockBase + lock_addr.offset;
  faa_ror.lkey = iCon->cacheLKey;
  faa_ror.remoteRKey = remoteInfo[lock_addr.nodeID].lockRKey[0];

  // Fetch the lock queue.
  RdmaOpRegion read_ror;
  read_ror.source = (uint64_t)hdr_buf + 8;
  read_ror.size = MAX_QUEUE_SIZE * sizeof(lqe);
  read_ror.lkey = iCon->cacheLKey;
  read_ror.dest =  remoteInfo[lock_addr.nodeID].lockBase + LQUEUE_OFF(lock_addr.offset);
  read_ror.remoteRKey = remoteInfo[lock_addr.nodeID].lockRKey[0];

  ibv_qp* qp = iCon->data[0][lock_addr.nodeID];
  uint64_t add_val = LMETA(-1, 0, -2);
  if (ctx == nullptr) {
    rdmaFaaRead(qp, faa_ror, add_val, read_ror, true);
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  } else {
    rdmaFaaRead(qp, faa_ror, add_val, read_ror, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
  DBG("release issued, fetch lock queue addr 0x%llx", read_ror.dest);
  }
  sherman_cql_release_check(lock_addr, core_id, hdr_buf, ctx);
  return 0;
}

int DSM::sherman_cql_release_batch(GlobalAddress lock_addr, uint32_t core_id, 
  uint64_t *hdr_buf, char *data_buf, GlobalAddress data_addr, size_t size,
  CoroContext* ctx) {

  // timer(LOCK_CQL_RELEASE, get_cur_lcore_id());

  {
  // timer(LOCK_CQL_RELEASE_ISSUE_BATCH, get_cur_lcore_id());
  // Write to the remote data item.
  RdmaOpRegion write_ror;
  write_ror.source = (uint64_t)data_buf;
  write_ror.size = size;
  write_ror.lkey = iCon->cacheLKey;
  write_ror.dest = remoteInfo[data_addr.nodeID].dsmBase + data_addr.offset;
  write_ror.remoteRKey = remoteInfo[data_addr.nodeID].dsmRKey[0];

  // Dequeue the acquirer.
  RdmaOpRegion faa_ror;
  faa_ror.source = (uint64_t)hdr_buf;
  faa_ror.lkey = iCon->cacheLKey;
  faa_ror.dest = remoteInfo[lock_addr.nodeID].lockBase + lock_addr.offset;
  faa_ror.remoteRKey = remoteInfo[data_addr.nodeID].lockRKey[0];

  // Fetch the lock queue.
  RdmaOpRegion read_ror;
  read_ror.source = (uint64_t)hdr_buf + 8;
  read_ror.size = MAX_QUEUE_SIZE * sizeof(lqe);
  read_ror.lkey = iCon->cacheLKey;
  read_ror.dest = remoteInfo[lock_addr.nodeID].lockBase +
    LQUEUE_OFF(lock_addr.offset);
  read_ror.remoteRKey = remoteInfo[data_addr.nodeID].lockRKey[0];

  ibv_qp *qp = iCon->data[0][lock_addr.nodeID];
  uint64_t add_val = LMETA(-1, 0, -2);
  if (ctx == nullptr) {
    rdmaWriteFaaRead(qp, write_ror, faa_ror, add_val, read_ror, true);
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  } else {
    rdmaWriteFaaRead(qp, write_ror, faa_ror, add_val, read_ror, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
  DBG("release batch issued, fetch lock queue addr 0x%llx", read_ror.dest);
  }

  sherman_cql_release_check(lock_addr, core_id, hdr_buf, ctx);
  return 0;
}

// After release the lock, check whether there are waiters for this lock.
void DSM::sherman_cql_release_check(GlobalAddress lock_addr, 
  uint32_t core_id, uint64_t *hdr_buf, CoroContext* ctx) {

  // timer(LOCK_CQL_RELEASE_CHECK, get_cur_lcore_id());
  int coro_id = (ctx == nullptr) ? 0 : ctx->coro_id;
  // Notify the first waiter.
  auto meta = *(uint64_t *)hdr_buf;
  DBG("release result: head %d size %d", LQ_HEAD(meta), LQ_SIZE(meta));
  auto queue = (lqe *)(hdr_buf + 1);
  if (LQ_SIZE(meta) > 1) {
    auto head = LQ_HEAD(meta) + 1;
    auto entry = &queue[head % MAX_QUEUE_SIZE];

    // Fetch the latest queue entry value.
    while (entry->ver != SHERMAN_VER(head)) {
      uint64_t source = (uint64_t)entry;
      uint64_t dest = remoteInfo[lock_addr.nodeID].lockBase + (uint64_t)LQE_OFF(lock_addr.offset, head % MAX_QUEUE_SIZE);
      uint64_t size = sizeof(lqe);
      uint32_t lkey = iCon->cacheLKey;
      uint32_t remoteRKey = remoteInfo[lock_addr.nodeID].lockRKey[0];
      rdmaRead(iCon->data[0][lock_addr.nodeID], source, dest, size, 
        lkey, remoteRKey, true, coro_id);

      if (ctx) (*ctx->yield)(*ctx->master);
      else {
        ibv_wc wc;
        pollWithCQ(iCon->cq, 1, &wc);
      }
      DBG("fetch lqe %d, mid %d tid %d version %d", 
        head, entry->mid, entry->tid, entry->ver);
    }

    // If the waiter is remote, send a two-sided notification.
    auto buf = (aql_notification *)rdma_sendbuf(MAKE_TID(core_id, coro_id),
      sizeof(aql_notification), NULL);
    buf->lock = lock_addr.offset;
    buf->ack = 0;
    buf->tid = entry->tid;
    buf->rwaiter_ts = entry->ts;
    rdma_send(core_id, entry->mid, TID_CORE(entry->tid), (char *)buf,
      sizeof(aql_notification), false);
    DBG("notified mid %d tid %d version %d at idx %d", 
      entry->mid, entry->tid, entry->ver, head);
  }
}

int sherman_wake_waiter(char* buf, qkey from, void* args) {
  auto notif = (aql_notification *)buf;
  auto wwargs = (wake_waiter_args *)args;
  auto core = wwargs->core_id;
  GlobalAddress lock_addr;
  lock_addr.offset = notif->lock;

  // Received grant signal.
  DBG("received grant signal from %lx, ts %d", from, notif->rwaiter_ts);
  // notif->rwaiter_ts = NO_TS;
  sherman_granted_locks[notif->tid][notif->lock] =
    TS(notif->rwaiter_ts);
  return 0;
}

int DSM::sherman_cql_is_granted(GlobalAddress lock_addr, uint32_t core_id,
  uint64_t *hdr_buf, CoroContext *ctx) {
  // DBG("begin to check whether cql is granted");
  int coro_id = (ctx == nullptr) ? 0 : ctx->coro_id;
  wake_waiter_args args = {
    .dsm = this,
    .lock_addr = lock_addr,
    .core_id = core_id,
    .qp = iCon->data[0][lock_addr.nodeID],
    .hdr_buf = hdr_buf,
  };
  rdma_poll_recvs(core_id, sherman_wake_waiter, (void *)(&args));
  
  auto tid = MAKE_TID(core_id, coro_id);
  auto ts = sherman_granted_locks[tid].find(lock_addr.offset);
  if (ts != sherman_granted_locks[tid].end()) {
    sherman_granted_locks[tid].erase(ts);
    DBG("lock is granted");
    return ts->second;
  }
  return 0;
}

/**
 * Fetch the lock header and the lock queue in one RDMA READ.
 */
void DSM::sherman_cql_fetch(GlobalAddress lock_addr, uint32_t core_id,
  uint64_t *hdr_buf, CoroContext* ctx) {
  ibv_qp* qp = iCon->data[0][lock_addr.nodeID];
  uint64_t source = (uint64_t)hdr_buf;
  uint64_t dest = remoteInfo[lock_addr.nodeID].lockBase + lock_addr.offset;
  uint64_t size = sizeof(aql);
  uint32_t lkey = iCon->cacheLKey;
  uint32_t remoteRKey = remoteInfo[lock_addr.nodeID].lockRKey[0];

  if (ctx == nullptr) {
    rdmaRead(qp, source, dest, size, lkey, remoteRKey, true);
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  } else {
    rdmaRead(qp, source, dest, size, lkey, remoteRKey, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}