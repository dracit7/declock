
#include <vector>
#include <atomic>
#include <unordered_set>
#include <unordered_map>
#include <cstdint>

#include <junction/ConcurrentMap_Leapfrog.h>

#include "statistics.h"
#include "debug.h"
#include "lm.h"
#include "conf.h"
#include "rdma.h"

#include "rlib/rdma_ctrl.hpp"
#include "rlib/pre_connector.hpp"
#include "scheduler/corotine_scheduler.h"
#include "dtx/dtx.h"

using std::vector;
using std::unordered_set;
using std::unordered_map;
using namespace std::chrono;

using ford_rdmaio::RCQP;
using ford_rdmaio::no_timeout;

// Lock granting.
junction::ConcurrentMap_Leapfrog<uint32_t, uint32_t>
  ford_granted_locks[MAX_CORE_NUM];
#define NO_TS 0xffffffff

// Lock aborting.
#define CQL_ABORTED      22
#define CQL_REACQUIRED   33
#define CQL_NOT_ACQUIRED 0xffffffff
unordered_map<uint32_t, int> ford_aborted_locks[MAX_CORE_NUM];
unordered_set<uint32_t> ford_torelease_locks[MAX_CORE_NUM];

// Lock grant ACK.
#define WAITER(lid, qkey) (((lid) << 20) ^ ((qkey) & 0xfffff))
#define WAITER_LID(waiter) ((waiter) >> 20)
#define WAITER_QKEY(waiter) ((waiter) & 0xfffff)
unordered_map<uint64_t, steady_clock::time_point> 
  sent_notifications[MAX_CORE_NUM];

#define CORE_ID(cid) ((cid) >> 3)
#define CRT_ID(cid) ((cid) & 0x7)

#define LQUEUE_OFF(addr) (addr + sizeof(lmeta))
#define LQE_OFF(addr, slot) (LQUEUE_OFF(addr) + (slot) * sizeof(lqe))

#define FORD_VER(i) (VERSION(i) + 1)

typedef struct {
  DTX* dtx;
  uint32_t cid;
  RCQP* qp;
  char* hdr_buf;
} wake_waiter_args;

#define DBG(fmt, ...) (0)// do {\
  fprintf(stderr, "[host%d][lock%lx][core%d]" fmt "\n", \
    LHID, lk_addr, get_cur_lcore_id(), ##__VA_ARGS__);\
  fflush(stderr);\
} while (0)

int ford_wake_waiter(char* buf, qkey from, void* args) {
  auto notif = (aql_notification *)buf;
  auto wwargs = (wake_waiter_args *)args;
  auto lk_addr = notif->lock;
  auto core = CORE_ID(wwargs->cid);

  // Received grant ACK.
  if (notif->ack) {
    DBG("received grant ACK from %lx", from);
    sent_notifications[core].erase(WAITER(lk_addr, from));

  // Received grant signal.
  } else {
    DBG("received grant signal from %lx", from);
    notif->rwaiter_ts = NO_TS;
    ford_granted_locks[core].assign(lk_addr, notif->rwaiter_ts);

    // If the lock has been aborted, release the granted lock.
    auto state = ford_aborted_locks[core].find(lk_addr);
    if (state != ford_aborted_locks[core].end()) {
      if (state->second == CQL_ABORTED) {
        DBG("release aborted lock");
        ford_torelease_locks[core].insert(lk_addr);
        ford_aborted_locks[core].erase(state);
        wwargs->dtx->ford_cql_release(lk_addr, wwargs->cid, 
          wwargs->qp, wwargs->hdr_buf, NULL, 0, 0);
        wwargs->dtx->ford_cql_release_check(lk_addr, wwargs->cid, 
          wwargs->qp, wwargs->hdr_buf, true);

      // If the lock is aborted and reacquired, grant the lock.
      } else if (state->second == CQL_REACQUIRED) {
        DBG("grant reacquired lock");
        ford_aborted_locks[core].erase(state);
      }
    }

    // Send ACK.
    auto buf = (aql_notification *)rdma_sendbuf(core, 
      sizeof(aql_notification), NULL);
    buf->lock = lk_addr;
    buf->ack = 1;
    rdma_send(core, MID(from), CID(from), (char *)buf,
      sizeof(aql_notification), false);
  }

  // Retry timeouted notifications.
  for (auto& notif : sent_notifications[core]) {
    if (duration_cast<std::chrono::milliseconds>(
        steady_clock::now() - notif.second).count() > 10) {

      auto buf = (aql_notification *)rdma_sendbuf(
          core, sizeof(aql_notification), NULL);
      auto to = WAITER_QKEY(notif.first);
      lk_addr = WAITER_LID(notif.first);
      DBG("timeout, notify %lx again", to);
      buf->lock = lk_addr;
      buf->ack = 0;
      buf->rwaiter_ts = 0;
      rdma_send(core, MID(to), CID(to), (char *)buf,
        sizeof(aql_notification), false);
      notif.second = steady_clock::now();
    }
  }

  return 0;
}

int DTX::ford_cql_acquire(uint64_t lk_addr, uint32_t cid, RCQP* qp, 
  char* hdr_buf, char* data_buf, uint64_t data_addr, size_t size) {
  DBG("acquire start");
  ford_torelease_locks[CORE_ID(cid)].insert(lk_addr);

  // Wait until the lock resumes from the aborted state.
  auto lk_aborted = ford_aborted_locks[CORE_ID(cid)].find(lk_addr);
  if (lk_aborted != ford_aborted_locks[CORE_ID(cid)].end()) {
    DBG("check aborted locks");
    lk_aborted->second = CQL_REACQUIRED;
    *(uint64_t *)hdr_buf = CQL_NOT_ACQUIRED;
    return 0;
  }

  ibv_wc wc{};
  ibv_send_wr sr[2];
  ibv_sge sge[2] = {
    {.addr = (uint64_t)hdr_buf, .length = 8, .lkey = qp->local_mr_.key},
    {.addr = (uint64_t)data_buf, .length = size, .lkey = qp->local_mr_.key},
  };

  // Enqueue the acquirer.
  sr[0].num_sge = 1;
  sr[0].sg_list = &sge[0];
  sr[0].send_flags = 0;
  sr[0].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  sr[0].next = &sr[1];
  sr[0].wr.atomic.remote_addr = qp->remote_mr_.buf + lk_addr;
  sr[0].wr.atomic.rkey = qp->remote_mr_.key;
  sr[0].wr.atomic.compare_add = LMETA(1, 0, 1);

  // Fetch the remote data.
  sr[1].num_sge = 1;
  sr[1].sg_list = &sge[1];
  sr[1].send_flags = IBV_SEND_SIGNALED;
  sr[1].opcode = IBV_WR_RDMA_READ;
  sr[1].next = NULL;
  sr[1].wr.rdma.remote_addr = qp->remote_mr_.buf + data_addr;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;

  ibv_send_wr* bad_sr;
  if (!coro_sched->RDMABatch(coro_id, qp, &sr[0], &bad_sr, 1)) {
    ERROR("[host%u]RDMABatch failed during acquire, lk_addr: 0x%lx, "
          "data_addr: 0x%lx, data size: 0x%x, remote_key: 0x%x",
          LHID, lk_addr, data_addr, size, qp->remote_mr_.key);
  }

  DBG("acquire issued");
  return 0;
}

int DTX::ford_cql_acquire_check(uint64_t lk_addr, uint32_t cid, RCQP* qp, 
  char* hdr_buf) {

  auto meta = *(uint64_t *)hdr_buf;
  DBG("acquire result: head %d size %d excl %d", LQ_HEAD(meta), 
    LQ_SIZE(meta), EXCL_CNT(meta));
  if (meta == CQL_NOT_ACQUIRED) return 0;

  // Is the lock directly granted?
  if (LQ_SIZE(meta) == 0) {
    ford_granted_locks[CORE_ID(cid)].assign(lk_addr, NO_TS);
    return 1;

  // If not, populate the queue entry.
  } else {
    auto idx = LQ_HEAD(meta) + LQ_SIZE(meta);
    auto entry = (lqe *)hdr_buf;
    entry->op = 1;
    entry->mid = LHID;
    entry->tid = cid;
    entry->ver = FORD_VER(idx);
    // entry->ts = timestamp_now(tid);

    DBG("fill lqe %d, mid %d tid %d ver %d", idx, LHID, cid, entry->ver);
    ibv_wc wc{};
    qp->post_send(IBV_WR_RDMA_WRITE, hdr_buf, sizeof(lqe),
      LQE_OFF(lk_addr, idx % MAX_QUEUE_SIZE), 0);
    // auto rc = qp->poll_till_completion(wc, no_timeout);
    // if (rc != SUCC) ERROR("failed to populate the queue entry.");
    
    return 0;
  }
} 

/* Returns 0 if the release RDMA request is issued, 1 otherwise.
 */
int DTX::ford_cql_release(uint64_t lk_addr, uint32_t cid, RCQP* qp, 
  char* hdr_buf, char* data_buf, uint64_t data_addr, size_t size) {

  auto release = ford_torelease_locks[CORE_ID(cid)].find(lk_addr);
  if (unlikely(release == ford_torelease_locks[CORE_ID(cid)].end())) {
    DBG("duplicated release");
    return 0;
  }

  // If the lock has been aborted, do not release the lock again.
  DBG("release start");
  auto lk_aborted = ford_aborted_locks[CORE_ID(cid)].find(lk_addr);
  if (unlikely(lk_aborted != ford_aborted_locks[CORE_ID(cid)].end())) {
    DBG("skip aborted lock");
    ford_torelease_locks[CORE_ID(cid)].erase(lk_addr);
    ford_granted_locks[CORE_ID(cid)].erase(lk_addr);
    ford_aborted_locks[CORE_ID(cid)][lk_addr] = CQL_ABORTED;
    return 1;
  }

  auto lk_granted = ford_granted_locks[CORE_ID(cid)].get(lk_addr);
  if (unlikely(!lk_granted)) {
    DBG("abort ungranted lock");
    ford_cql_abort(lk_addr, cid, qp, hdr_buf);
    ford_granted_locks[CORE_ID(cid)].erase(lk_addr);
    return 1;
  }

  ibv_wc wc{};
  ibv_send_wr sr[3];
  ibv_sge sge[3] = {
    {.addr = (uint64_t)data_buf, .length = size, .lkey = qp->local_mr_.key},
    {.addr = (uint64_t)hdr_buf, .length = 8, .lkey = qp->local_mr_.key},
    {.addr = (uint64_t)hdr_buf + 8, .length = MAX_QUEUE_SIZE * sizeof(lqe), 
     .lkey = qp->local_mr_.key},
  };

  // Write to the remote data item.
  sr[0].num_sge = 1;
  sr[0].sg_list = &sge[0];
  sr[0].send_flags = 0;
  sr[0].opcode = IBV_WR_RDMA_WRITE;
  sr[0].next = &sr[1];
  sr[0].wr.rdma.remote_addr = qp->remote_mr_.buf + data_addr;
  sr[0].wr.rdma.rkey = qp->remote_mr_.key;

  // Dequeue the acquirer.
  sr[1].num_sge = 1;
  sr[1].sg_list = &sge[1];
  sr[1].send_flags = 0;
  sr[1].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  sr[1].next = &sr[2];
  sr[1].wr.atomic.remote_addr = qp->remote_mr_.buf + lk_addr;
  sr[1].wr.atomic.rkey = qp->remote_mr_.key;
  sr[1].wr.atomic.compare_add = LMETA(-1, 0, -2);

  // Fetch the lock queue.
  sr[2].num_sge = 1;
  sr[2].sg_list = &sge[2];
  sr[2].send_flags = IBV_SEND_SIGNALED;
  sr[2].opcode = IBV_WR_RDMA_READ;
  sr[2].next = NULL;
  sr[2].wr.rdma.remote_addr = qp->remote_mr_.buf + LQUEUE_OFF(lk_addr);
  sr[2].wr.rdma.rkey = qp->remote_mr_.key;

  // Do not issue the WRITE if the caller only wants to unlock.
  int s = (size == 0) ? 1 : 0;
  ibv_send_wr* bad_sr;
  if (!coro_sched->RDMABatch(coro_id, qp, &sr[s], &bad_sr, 2 - s)) {
    ERROR("[host%u]RDMABatch failed during release, lk_addr: 0x%lx, "
          "data_addr: 0x%lx", LHID, lk_addr, data_addr);
  }

  ford_granted_locks[CORE_ID(cid)].erase(lk_addr);
  return 0;
}

void DTX::ford_cql_release_check(uint64_t lk_addr, uint32_t cid, RCQP* qp, 
  char* hdr_buf, bool yield) {

  auto release = ford_torelease_locks[CORE_ID(cid)].find(lk_addr);
  if (release == ford_torelease_locks[CORE_ID(cid)].end()) {
    DBG("duplicated release");
    return;
  }
  ford_torelease_locks[CORE_ID(cid)].erase(lk_addr);

  // Yield to the background coroutine for polling RDMA wc.
  if (yield) coro_sched->Yield(*sched_func, coro_id);

  // Notify the first waiter.
  auto meta = *(uint64_t *)hdr_buf;
  DBG("release result: head %d size %d", LQ_HEAD(meta), LQ_SIZE(meta));
  auto queue = (lqe *)(hdr_buf + 8);
  if (LQ_SIZE(meta) > 1) {
    auto head = LQ_HEAD(meta) + 1;
    auto entry = &queue[head % MAX_QUEUE_SIZE];

    // Fetch the latest queue entry value.
    while (entry->ver != FORD_VER(head)) {
      coro_sched->RDMAReadSync(coro_id, qp, (char *)entry,
        LQE_OFF(lk_addr, head % MAX_QUEUE_SIZE), sizeof(lqe));
      DBG("fetch lqe %d, mid %d tid %d version %d", 
        head, entry->mid, entry->tid, entry->ver);
    }

    // If the waiter is local, directly record the notification.
    if (false) {
    // if (entry->mid == LHID) {
      entry->ts = NO_TS;
      ford_granted_locks[CORE_ID(entry->tid)].assign(lk_addr, entry->ts);
      DBG("notified localhost tid %d", entry->tid);

    // If the waiter is remote, send a two-sided notification.
    } else {
      auto buf = (aql_notification *)rdma_sendbuf(
          CORE_ID(cid), sizeof(aql_notification), NULL);
      buf->lock = lk_addr;
      buf->ack = 0;
      buf->rwaiter_ts = entry->ts;
      rdma_send(CORE_ID(cid), entry->mid, CORE_ID(entry->tid), (char *)buf,
        sizeof(aql_notification), false);
      DBG("notified mid %d tid %d", entry->mid, entry->tid);

      auto waiter = WAITER(lk_addr, QKEY(entry->mid, CORE_ID(entry->tid)));
      sent_notifications[CORE_ID(cid)][waiter] = steady_clock::now();
    }
  }
}

void DTX::ford_cql_fetch(uint64_t lk_addr, uint32_t cid, RCQP* qp, char* buf) {
  coro_sched->RDMAReadSync(coro_id, qp, buf, lk_addr, sizeof(aql));
}

int DTX::ford_cql_is_granted(uint64_t lk_addr, uint32_t cid, RCQP* qp, 
  char* hdr_buf) {

  wake_waiter_args args = {
    .dtx = this,
    .cid = cid,
    .qp = qp,
    .hdr_buf = hdr_buf,
  };
  rdma_poll_recvs(CORE_ID(cid), ford_wake_waiter, (void *)(&args));

  auto ts = ford_granted_locks[CORE_ID(cid)].get(lk_addr);
  if (ts) {
    DBG("lock is granted");
    return 1 ^ (ts << 1);
  }
  return 0;
}

int DTX::ford_cql_abort(uint64_t lk_addr, uint32_t cid, RCQP* qp, char* buf) {
  DBG("abort lock");
  ford_aborted_locks[CORE_ID(cid)][lk_addr] = CQL_ABORTED;
  return 0;
}
