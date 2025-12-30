
#include <sstream>
#include <fstream>
#include <string>
#include <string.h>
#include <vector>
#include <unordered_map>
#include <streambuf>
#include <cstdint>

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>
#include "dirent.h"

#include "debug.h"
#include "conf.h"
#include "rdma.h"
#include "statistics.h"
#include "util.h"

#include "core/lib.hh"
#include "core/qps/recv_iter.hh"
#include "core/qps/doorbell_helper.hh"

using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;
using namespace std::chrono;

using std::ostringstream;
using std::ifstream;
using std::unordered_map;
using std::vector;

static const enum counter_type ctr_t[IBV_WR_SEND_WITH_INV] = {
  [IBV_WR_RDMA_WRITE] = CTR_RDMA_WRITE,
  [IBV_WR_RDMA_WRITE_WITH_IMM] = CTR_TYPE_MAX,
  [IBV_WR_SEND] = CTR_TYPE_MAX,
  [IBV_WR_SEND_WITH_IMM] = CTR_TYPE_MAX,
  [IBV_WR_RDMA_READ] = CTR_RDMA_READ,
  [IBV_WR_ATOMIC_CMP_AND_SWP] = CTR_RDMA_CAS,
  [IBV_WR_ATOMIC_FETCH_AND_ADD] = CTR_RDMA_FA,
};

class SimpleAllocator : AbsRecvAllocator {
  RMem::raw_ptr_t buf = nullptr;
  usize total_mem = 0;
  mr_key_t key;

public:
  SimpleAllocator(Arc<RMem> mem, mr_key_t key): 
    buf(mem->raw_ptr), total_mem(mem->sz), key(key) {}

  Option<std::pair<rmem::RMem::raw_ptr_t, rmem::mr_key_t>>
  alloc_one(const usize &sz) override {
    if (total_mem < sz)
      return {};
    auto ret = buf;
    buf = static_cast<char *>(buf) + sz;
    total_mem -= sz;
    return std::make_pair(ret, key);
  }

  Option<std::pair<rmem::RMem::raw_ptr_t, rmem::RegAttr>>
  alloc_one_for_remote(const usize &sz) override {
    return {};
  }
};

// The RPC daemon on this server.
static Arc<RCtrl> ctrl;

// Per-core QPs and MRs.
static Arc<UD>         ud_qp[MAX_CORE_NUM];
static Arc<RegHandler> send_mr[MAX_CORE_NUM];
static Arc<RegHandler> recv_mr[MAX_CORE_NUM];
static Arc<RegHandler> rc_mr[MAX_CORE_NUM];
static size_t          send_buf_off[MAX_CORE_NUM];

// Per-core RECV buffer.
static Arc<RecvEntries<RDMA_RECV_ENTRY_NUM>> recv_buf[MAX_CORE_NUM];

// Remote QP attributes.
static QPAttr remote_qp_attr[MAX_CORE_NUM][MAX_CN_NUM + MAX_MN_NUM];
static ibv_ah* remote_qp_ah[MAX_CORE_NUM][MAX_CN_NUM + MAX_MN_NUM];

// RC QPs and local MRs and remote MRs.
// TODO: We assume that the number of memory node is 1,
// which may change in the future.
Arc<RC> dmlock_rc_qp[MAX_MN_NUM][MAX_CORE_NUM];
RegAttr local_mr_map[MAX_MN_NUM][MAX_CORE_NUM];
RegAttr remote_mr_map[MAX_MN_NUM][MAX_CORE_NUM];

// The buffer on MN to be accessed by RDMA verbs.
static char* mn_buf;

// Poll completion counters.
uint32_t completion_cnt[MAX_CLIENT_NUM];

/**
 * Poll latest recv work-completions and process each received
 * post with a hook function.
 *
 * Returns the number of posts received.
 */
int rdma_poll_recvs(size_t core, pkt_processor proc, void* args) {
  size_t polled_packet_cnt = 0;

  // Poll all completed recv work-completions (wcs) into `recv_buf`.
  RecvIter<UD, RDMA_RECV_ENTRY_NUM> iter(ud_qp[core], recv_buf[core]);
  for (; iter.has_msgs(); iter.next(), polled_packet_cnt++) {

    // Extract the received packet address from the wc.
    auto imm_msg = iter.cur_msg().value();
    auto buf = static_cast<char *>(std::get<1>(imm_msg)) + kGRHSz;
    auto from_qkey = std::get<0>(imm_msg);

    // Call the packet processor to handle the packet.
    proc(buf, from_qkey, args);
  }

  // When deconstructed, RecvIter re-fills the RQ by posting
  // the equal amount of wrs automatically.
  return polled_packet_cnt > 0;
}

/**
 * Returns the send buffer of a specified core.
 */
char* rdma_sendbuf(uint32_t tid, uint32_t size, bool* sync) {
  auto base = (char *)send_mr[TID_CORE(tid)]->get_reg_attr().value().buf;
  auto off = send_buf_off[TID_CORE(tid)];

  send_buf_off[TID_CORE(tid)] += size;
  if (send_buf_off[TID_CORE(tid)] >= RDMA_SEND_MR_SIZE) {
    off = 0;
    send_buf_off[TID_CORE(tid)] = size;
    if (sync) *sync = true;
  } else if (sync) *sync = false;

  return base + off;
}

/**
 * Returns the read/write buffer of a specified core.
 */
char* rdma_rc_buf(uint32_t tid) {
  return (char*)(rc_mr[TID_CORE(tid)]->get_reg_attr().value().buf +
    TID_CRT(tid) * RDMA_CLIENT_RC_MR_SIZE);
}

/**
 * Returns the buffer on MN to be accessed by remote RDMA verbs.
 */
char* rdma_mn_buf() {
  return mn_buf;
}

/* Send a post using RDMA (zero-copy).
 *
 * When calling this function, one must ensure that `buf' is
 * returned from rdma_sendbuf(), otherwise the result is undefined.
 */
int rdma_send(uint16_t core, uint16_t dest_mid, uint16_t dest_cid, 
              char* buf, size_t size, bool sync) {

  // Must poll cq regularly, or send queue will overflow.
  if (add_counter(core, CTR_RDMA_SEND) % 128 == 0) sync = true;

  ibv_sge sge;
  sge.addr = (uintptr_t)(buf);
  sge.length = size + 1;
  sge.lkey = send_mr[core]->get_reg_attr().value().key;

  ibv_send_wr wr;
  wr.opcode = IBV_WR_SEND_WITH_IMM;
  wr.num_sge = 1;
  wr.imm_data = QKEY(LHID, core);
  wr.next = nullptr;
  wr.send_flags = sync ? IBV_SEND_SIGNALED : 0;
  wr.sg_list = &sge;

  wr.wr.ud.ah = remote_qp_ah[dest_cid][dest_mid];
  wr.wr.ud.remote_qpn = remote_qp_attr[dest_cid][dest_mid].qpn;
  wr.wr.ud.remote_qkey = remote_qp_attr[dest_cid][dest_mid].qkey;

  struct ibv_send_wr *bad_sr = nullptr;
  auto ret = ibv_post_send(ud_qp[core]->qp, &wr, &bad_sr);
  ASSERT_MSG(ret == 0, "error code %d", ret);

  if (sync) {
    auto ret_r = ud_qp[core]->wait_one_comp();
    LASSERT(ret_r == IOCode::Ok);
  }
  return 0;
}

/* Send a batch of two-sided RDMA posts.
 */
int rdma_send_batch(uint32_t tid, size_t N, sreq_desc* desc, bool sync) {
  auto core = TID_CORE(tid);
  ibv_sge sge[RDMA_MAX_BATCH_SIZE];
  ibv_send_wr wr[RDMA_MAX_BATCH_SIZE];

  for (int i = 0; i < N; i++) {
    sge[i].addr = (uintptr_t)(desc[i].local_addr);
    sge[i].length = desc[i].len;
    sge[i].lkey = send_mr[core]->get_reg_attr().value().key;

    wr[i].opcode = IBV_WR_SEND_WITH_IMM;
    wr[i].num_sge = 1;
    wr[i].imm_data = QKEY(LHID, core);
    wr[i].next = (i == N - 1) ? NULL : &wr[i + 1];
    wr[i].send_flags = (i == N - 1 && sync) ? IBV_SEND_SIGNALED : 0;
    wr[i].sg_list = &sge[i];

    wr[i].wr.ud.ah = remote_qp_ah[desc[i].cid][desc[i].mid];
    wr[i].wr.ud.remote_qpn = remote_qp_attr[desc[i].cid][desc[i].mid].qpn;
    wr[i].wr.ud.remote_qkey = remote_qp_attr[desc[i].cid][desc[i].mid].qkey;

    add_counter(core, CTR_RDMA_SEND);
  }

  struct ibv_send_wr *bad_sr = nullptr;
  auto ret = ibv_post_send(ud_qp[core]->qp, wr, &bad_sr);
  ASSERT_MSG(ret == 0, "error code %d", ret);

  if (sync) ud_qp[core]->wait_one_comp();
  return 0;
}

/**
 * Generic one-sided RDMA operation.
 * If called with wait_comp == false, the caller must call
 * rdma_wait_one() before issuing any other RDMA request.
 *
 * Supported ops: READ, WRITE, FA, CAS
 */
int rdma_one_sided_op(char opcode, uint32_t tid, uint16_t dest, char* local_addr, 
  uint64_t remote_addr, uint32_t size, bool wait_comp, uint64_t compare_add,
  uint64_t swap) {
  auto core = TID_CORE(tid);

  auto res_s = dmlock_rc_qp[dest][core]->send_normal(
    {
      .op = (ibv_wr_opcode)opcode,
      .flags = IBV_SEND_SIGNALED,
      .len = size,
      .wr_id = tid,
    },
    {
      .local_addr = reinterpret_cast<RMem::raw_ptr_t>(local_addr),
      .remote_addr = remote_addr,
      .imm_data = tid,
      .compare_add = compare_add,
      .swap = swap,
    }
  );
  LASSERT(res_s == IOCode::Ok);

  if (wait_comp) {
    auto res_p = dmlock_rc_qp[dest][core]->wait_one_comp();
    LASSERT(res_p == IOCode::Ok);
  }

  add_counter(core, ctr_t[opcode]);
  return 0;
}

/**
 * Send a batch of one-sided RDMA operations.
 */
int rdma_one_sided_batch(uint32_t tid, uint16_t dest, size_t N, rreq_desc* desc) {
  auto core = TID_CORE(tid);

  DoorbellHelper<RDMA_MAX_BATCH_SIZE> batch(IBV_WR_RDMA_READ);
  for (size_t i = 0; i < N; i++) {
    batch.next();
    batch.cur_sge() = {
      .addr = (u64)(desc[i].local_addr), 
      .length = desc[i].len,
      .lkey = local_mr_map[dest][core].lkey
    };

    ibv_send_wr& sr = batch.cur_wr();
    sr.wr_id = dmlock_rc_qp[dest][core]->encode_my_wr(tid, 1);
    sr.opcode = desc[i].op;
    sr.send_flags = 0;

    if (desc[i].compare_add == 0 && desc[i].swap == 0) {
      sr.wr.rdma.remote_addr = remote_mr_map[dest][core].buf +
                               desc[i].remote_addr;
      sr.wr.rdma.rkey = remote_mr_map[dest][core].key;
    } else {
      sr.wr.atomic.remote_addr = remote_mr_map[dest][core].buf +
                                 desc[i].remote_addr;
      sr.wr.atomic.compare_add = desc[i].compare_add;
      sr.wr.atomic.swap = desc[i].swap;
      sr.wr.atomic.rkey = remote_mr_map[dest][core].key;
    }

    add_counter(core, ctr_t[sr.opcode]);
  }

  batch.cur_wr().send_flags = IBV_SEND_SIGNALED;
  dmlock_rc_qp[dest][core]->out_signaled += 1;

  struct ibv_send_wr *bad_sr;
  batch.freeze();
  return ibv_post_send(dmlock_rc_qp[dest][core]->qp, batch.first_wr_ptr(), &bad_sr);
}

/**
 * Wait until the last issued rdma operation finishes.
 */
void rdma_wait_one(uint16_t dest, uint32_t tid) {
  while (!rdma_poll_one(dest, tid)) ;
}

/**
 * Check whether there is one completed RDMA request.
 */
bool rdma_poll_one(uint16_t dest, uint32_t tid) {
  while (1) {

    // If there are existing completed op, directly return.
    if (completion_cnt[tid]) {
      completion_cnt[tid]--;
      return true;
    }

    // Poll one completion, return false if there are no completions.
    auto res_p = dmlock_rc_qp[dest][TID_CORE(tid)]->poll_rc_comp();
    if (!res_p) return false;

    // If the polled completion does not belong to the current coroutine,
    // record the completion and poll again.
    if (tid == res_p.value().first) return true;
    else completion_cnt[res_p.value().first]++;
  }

  return false;
}

/**
 * Set up the RDMA communication layer on CN.
 * 
 * Caller should specify the NIC to use and a bitmap containing CPU cores
 * that require RC or UD communication facility. Note that cores should be
 * on the same NUMA node with NIC to gain optimal performance. 
 */
int rdma_setup_cn(uint64_t nic_idx, bitmap core_map) {

  // Collect the list of MNs from configuration.
  bitmap mn_map = 0;
  for (auto const& mn : MN_LIST) {
    BITMAP_SET(mn_map, mn);
  }

  // Initialize the RPC server that hosts the attributes of
  // local QPs and MRs. We use a different port for each RNIC to avoid
  // port collision between NUMA nodes.
  ctrl = Arc<RCtrl>(new RCtrl(RIB_RCTRL_PORT(LHID), MACHINES[LHID]));

  // Find the RNIC to use. Application should specify the RNIC
  // on the same NUMA node with CPU cores.
  auto nic = RNic::create(RNicInfo::query_dev_names().at(nic_idx)).value();

  // For each core that requires RDMA send/recv, register:
  // 
  // 1. A memory region divided into recvs for receiving RDMA posts.
  // 2. A memory region for sending RDMA posts.
  // 3. An UD QP for sending and receiving RDMA posts.
  // 
  // To support RDMA read/write, also register:
  // 
  // 4. A memory region for reading from or writing to remote memory.
  for (int i = 0; i < MAX_CORE_NUM; i++) {
    if (!BITMAP_CONTAIN(core_map, i)) continue;

    // Prepare the RECV memory region and the corresponding allocator.
    // The allocator splits the memory region to several RECV entries,
    // each having idential size to the send buffer.
    auto recv_mr_mem = Arc<RMem>(new RMem(RDMA_RECV_MR_SIZE));
    recv_mr[i] = RegHandler::create(recv_mr_mem, nic).value();

    SimpleAllocator alloc(recv_mr_mem, recv_mr[i]->get_reg_attr().value().key);
    recv_buf[i] = RecvEntriesFactory<SimpleAllocator, 
      RDMA_RECV_ENTRY_NUM, RDMA_RECV_ENTRY_SIZE>::create(alloc);
    recv_buf[i]->sanity_check();

    // Register the MR to the RPC server.
    ctrl->registered_mrs.reg(i, recv_mr[i]);

    // Create the UD QP for this core.
    ud_qp[i] = UD::create(nic, QPConfig().set_qkey(QKEY(LHID, i))).value();

    // Register the attributes of UD QP in the RPC daemon.
    ctrl->registered_qps.reg(QPNAME_UD(LHID, i), ud_qp[i]);

    // Post RECV entries to the UD QP.
    auto res = ud_qp[i]->post_recvs(*(recv_buf[i]), RDMA_RECV_ENTRY_NUM);
    ASSERT_MSG(res == IOCode::Ok, "error code %d", res.desc);

    // Also create the SEND and RC MRs for this core.
    send_mr[i] = RegHandler::create(
      Arc<RMem>(new RMem(RDMA_SEND_MR_SIZE)), nic).value();
    rc_mr[i] = RegHandler::create(
      Arc<RMem>(new RMem(RDMA_CN_RC_MR_SIZE)), nic).value();
  }

  // Start the RPC daemon (also wait for other machines to start theirs).
  auto ret = ctrl->start_daemon();
  LLOG("[host%u]Compute node RPC daemon (%s:%d) %s", LHID, MACHINES[LHID],
    RIB_RCTRL_PORT(LHID), ret ? "started" : "failed to start");
  sleep(5);

  // For each machine in the cluster:
  // 
  // 1. CN: fetch the UD QP attributes for each core on the machine.
  // 2. MN: establish the connection with the machine for each local core.
  for (size_t mid = 0; mid < MACHINES.size(); mid++) {

    // For local host, we do not need to fetch attributes through RPC.
    if (mid == LHID) {
      for (int core = 0; core < MAX_CORE_NUM; core++) {
        if (!BITMAP_CONTAIN(core_map, core)) continue;

        auto attr = ud_qp[core]->my_attr();
        remote_qp_attr[core][mid] = attr;
        remote_qp_ah[core][mid] = ud_qp[core]->create_ah(attr);
      }

      continue;
    }

    // Try to connect to the machine's RPC daemon.
    ostringstream addr;
    addr << MACHINES[mid] << ":" << RIB_RCTRL_PORT(mid);
    ConnectManager cm(addr.str());
    ERROR_IF(cm.wait_ready(100000, 4) == IOCode::Timeout, 
      "[host%u]connect to %s timeout", LHID, addr.str().c_str());
    
    // For each core that requires network communication:
    for (int core = 0; core < MAX_CORE_NUM; core++) {
      if (!BITMAP_CONTAIN(core_map, core)) continue;

      // Fetch UD QP attributes from each CN's RPC daemon.
      if (!BITMAP_CONTAIN(mn_map, mid)) {
        auto fetch_qp_attr_res = cm.fetch_qp_attr(QPNAME_UD(mid, core));
        ASSERT_MSG(fetch_qp_attr_res == IOCode::Ok, 
          "fetch qp %s attr error: %s", QPNAME_UD(mid, core).c_str(),
          std::get<0>(fetch_qp_attr_res.desc).c_str());

        auto attr = std::get<1>(fetch_qp_attr_res.desc);
        remote_qp_attr[core][mid] = attr;
        remote_qp_ah[core][mid] = ud_qp[0]->create_ah(attr);

      // Establish a reliable connection with each MN.
      } else {
        dmlock_rc_qp[mid][core] = RC::create(nic, QPConfig()).value();
        
        auto qp_res = cm.cc_rc(QPNAME_RC(LHID, core), dmlock_rc_qp[mid][core], 
          RDMA_NIC_KEY, QPConfig());
        LASSERT(qp_res == IOCode::Ok);
        auto key = std::get<1>(qp_res.desc);

        auto fetch_res = cm.fetch_remote_mr(RDMA_RC_MR_KEY);
        LASSERT(fetch_res == IOCode::Ok);

        dmlock_rc_qp[mid][core]->bind_remote_mr(std::get<1>(fetch_res.desc));
        dmlock_rc_qp[mid][core]->bind_local_mr(rc_mr[core]->get_reg_attr().value());

        local_mr_map[mid][core] = dmlock_rc_qp[mid][core]->local_mr.value();
        remote_mr_map[mid][core] = dmlock_rc_qp[mid][core]->remote_mr.value();
      }
    }
  }

  LLOG("[host%u]Set up compute node RDMA", LHID);
  return 0;
}

/**
 * Set up the RDMA communication layer on MN.
 */
int rdma_setup_mn(uint64_t nic_idx, uint64_t mr_size) {

  // Initialize the RPC daemon and NIC like rdma_setup_cn().
  ctrl = Arc<RCtrl>(new RCtrl(RIB_RCTRL_PORT(LHID), "0.0.0.0"));
  auto nic = RNic::create(RNicInfo::query_dev_names().at(nic_idx)).value();

  // Register the NIC in the RPC daemon.
  ctrl->opened_nics.reg(RDMA_NIC_KEY, nic);

  // Create and register the MR on MN.
  ctrl->registered_mrs.create_then_reg(
    RDMA_RC_MR_KEY, Arc<RMem>(new RMem(mr_size)), nic);

  // Save the buffer address.
  mn_buf = (char *)ctrl->registered_mrs.query(RDMA_RC_MR_KEY).value()
                       ->get_reg_attr().value().buf;
  
  ctrl->start_daemon();

  LLOG("[host%u]Set up memory node RDMA, mr_size %lu", LHID, mr_size);
  LLOG("[host%u]Memory node started RPC daemon on port %d", LHID, RIB_RCTRL_PORT(LHID));

  return 0;
}
