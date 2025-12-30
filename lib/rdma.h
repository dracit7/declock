
#ifndef __DMLOCK_RDMA_H
#define __DMLOCK_RDMA_H

/**
 * The RDMA communication layer.
 * 
 * The communication layer assumes the disaggregated memory architecture,
 * i.e., there are compute nodes (CN) and memory nodes (MN). CN use UD to
 * communicate with each other, and use RC to access MN memory.
 */

#include <vector>

#include "util.h"
#include "infiniband/verbs.h"

using std::vector;

/* The qkey and name of the qp for core x on machine y. */
typedef uint32_t qkey;
#define QKEY(mid, core) (uint32_t)(((mid + 1) << 16) + (core))
#define MID(qkey) (uint16_t)(((qkey) >> 16) - 1)
#define CID(qkey) (uint16_t)((qkey) & 0xffff)
#define QPNAME(prefix, mid, core) ({\
  ostringstream _name;\
  _name << prefix "_host" << mid << "_core" << core;\
  (_name.str());\
})

#define QPNAME_RC(mid, core) QPNAME("rc", mid, core)
#define QPNAME_UD(mid, core) QPNAME("ud", mid, core)

/* The packet processor hook for rdma_poll_recvs(). */
typedef int (*pkt_processor)(char* buf, qkey from, void* args);

/* Acquire the local buffer for storing RDMA post. */
extern "C" {
char* rdma_sendbuf(uint32_t, uint32_t, bool*);
char* rdma_rc_buf(uint32_t);
char* rdma_mn_buf();
}

/* Two-sided APIs. */
int rdma_send(uint16_t core, uint16_t dest_mid, uint16_t dest_cid, 
              char* buf, size_t size, bool sync);
int rdma_poll_recvs(size_t core, pkt_processor proc, void* args);

/* One-sided APIs. */
#define rdma_read(tid, dest, laddr, raddr, size, wait)\
  rdma_one_sided_op(IBV_WR_RDMA_READ, \
    tid, dest, laddr, raddr, size, wait, 0, 0)
#define rdma_write(tid, dest, laddr, raddr, size, wait)\
  rdma_one_sided_op(IBV_WR_RDMA_WRITE, \
    tid, dest, laddr, raddr, size, wait, 0, 0)
#define rdma_fa(tid, dest, laddr, raddr, compare_add, wait)\
  rdma_one_sided_op(IBV_WR_ATOMIC_FETCH_AND_ADD, \
    tid, dest, laddr, raddr, sizeof(uint64_t), wait, compare_add, 0)
#define rdma_cas(tid, dest, laddr, raddr, expected, swap, wait)\
  rdma_one_sided_op(IBV_WR_ATOMIC_CMP_AND_SWP, \
    tid, dest, laddr, raddr, sizeof(uint64_t), wait, expected, swap)

int rdma_one_sided_op(char opcode, uint32_t tid, uint16_t dest, char* local_addr, 
  uint64_t remote_addr, uint32_t size, bool wait_comp, uint64_t compare_add,
  uint64_t swap);
void rdma_wait_one(uint16_t dest, uint32_t tid);
bool rdma_poll_one(uint16_t dest, uint32_t tid);

/* Doorbell batching APIs. */
typedef struct {
  ibv_wr_opcode op;
  uint32_t      len;
  uint64_t      local_addr;
  uint64_t      remote_addr;
  uint64_t      compare_add;
  uint64_t      swap;
} rreq_desc;

int rdma_one_sided_batch(uint32_t tid, uint16_t dest, size_t N, rreq_desc* desc);

typedef struct {
  uint32_t  mid;
  uint32_t  cid;
  uint32_t  len;
  uint64_t  local_addr;
} sreq_desc;

int rdma_send_batch(uint32_t tid, size_t N, sreq_desc* desc, bool sync);

/* Setup the RDMA communication layer. */
extern "C" {
int rdma_setup_cn(uint64_t nic_idx, bitmap core_map);
int rdma_setup_mn(uint64_t nic_idx, uint64_t mr_size);
}

#endif