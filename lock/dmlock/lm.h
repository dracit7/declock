
#ifndef __DMLOCK_LM_H
#define __DMLOCK_LM_H

#include "common.h"

/**
 * Lock OP.
 */
#define LOCK_SHARED 0
#define LOCK_EXCL   1

/**
 * Definition of the lock metadata.
 * 
 *     M+N          N           N         1
 * +----------+-----------+------------+-----+
 * | lq_head  | lq_size   | excl_cnt   | rst |
 * +----------+-----------+------------+-----+
 * 
 * Note that lq_head could overflow (because of the ring buffer), 
 * so we must put it at the beginning of the 64 bits.
 * 
 * - excl_cnt: The number of exclusive requests in lock queue;
 * - lq_head: head index of the lock queue;
 * - lq_size: size of the lock queue.
 * 
 */
typedef uint64_t lmeta;

// Configurable factors that controls the lock header layout.
#if defined(APP_FORD)
#define CAPACITY_BITS 5
#elif defined(APP_SHERMAN)
#define CAPACITY_BITS 5
#else

#ifdef USE_COHORT
#define CAPACITY_BITS 5    /* log_2(queue capacity)  */
#else
#define CAPACITY_BITS 9
#endif
#endif

#define HEAD_BITS     24
#define SIZE_BITS     (CAPACITY_BITS + 1)
#define EXCNT_BITS    (CAPACITY_BITS + 1)

// Macros for parsing and packing the header.
#define MAX_QUEUE_SIZE   (1 << CAPACITY_BITS)
#define MAX_QUEUE_SIZE_B MAX_QUEUE_SIZE * sizeof(lqe)

#define BITMASK(bits) ((uint32_t)(-1) >> (32 - bits))
#define HEAD_MASK BITMASK(HEAD_BITS)
#define SIZE_MASK BITMASK(SIZE_BITS)
#define EXCNT_MASK BITMASK(EXCNT_BITS)

#define EXCL_CNT(lmeta) (((lmeta) >> 1) & EXCNT_MASK) 
#define LQ_SIZE(lmeta) (((lmeta) >> (EXCNT_BITS + 1)) & SIZE_MASK)
#define LQ_HEAD(lmeta) (((lmeta) >> (SIZE_BITS + EXCNT_BITS + 1)) & HEAD_MASK)

#define RST_BIT(lmeta) ((lmeta) & 1)
#define RST_CNT(lmeta) ((lmeta) >> (HEAD_BITS + SIZE_BITS + EXCNT_BITS + 1))
#define RST_LMETA(cnt) \
  ((uint64_t)(cnt) << (HEAD_BITS + SIZE_BITS + EXCNT_BITS + 1))

#define LMETA(excl_cnt, head, size) \
  (lmeta)(\
    (((excl_cnt) & EXCNT_MASK) << 1) ^ \
    (((head) & HEAD_MASK) << (SIZE_BITS + EXCNT_BITS + 1)) ^ \
    (((size) & SIZE_MASK) << (EXCNT_BITS + 1)) \
  )

/**
 * Definition of the lock queue entry.
 * 
 * Each lock queue entry is 32-bit sized. The sequence number
 * is used to represent the freshness of the entry, so that
 * the reader knows whether the entry content is up to date.
 */

#if defined(APP_FORD)
#define MID_BITS 2
#define TID_BITS 8
#else
#define MID_BITS 6 /* sizeof(CN_NUM + MN_NUM) */
#define TID_BITS 8
#endif

#define VERSION_BITS (HEAD_BITS - CAPACITY_BITS)  
#define VERSION(idx) (((idx) / MAX_QUEUE_SIZE) & BITMASK(VERSION_BITS))

#define TIMESTAMP_BITS 14
#define TIMESTAMP_BITMASK (BITMASK(TIMESTAMP_BITS))

typedef struct __attribute__((packed)) {
  unsigned char op:  1;  /* SHARED or EXCLUSIVE */
  unsigned char mid: MID_BITS;  /* Machine ID of requester */
  unsigned int  tid: TID_BITS;  /* Thread ID of requester */
  unsigned int  ver: VERSION_BITS; /* Version number */
#ifdef USE_TIMESTAMP
  unsigned int  ts:  TIMESTAMP_BITS; /* Timestamp */
#endif
} lqe;

/**
 * The lock grant notification packet format.
 */
typedef struct __attribute__((packed)) {
  unsigned int  ack:        1;
  unsigned int  rst:        1;
  unsigned int  lock:       30;
  unsigned int  tid:        16;
  unsigned int  rst_cnt:    16;
#ifdef USE_TIMESTAMP
  unsigned int  rwaiter_ts: 32;
#endif
} aql_notification;

#define NOTIF_BATCH_SIZE (RDMA_SEND_MR_SIZE / sizeof(aql_notification))
#define MAX_FETCH_COUNT 0x2000

/**
 * The global advisory queue-notify lock table layout.
 * 
 * Lock header and lock queue of each lock are placed adjacently
 * to be fetched in one single RDMA READ.
 */
typedef struct {
  lmeta hdr;
  lqe queue[MAX_QUEUE_SIZE];
} aql; /* Advisory Queue-notify Lock */

#define HDR_ADDR(lid) ((lid) * sizeof(aql))
#define LQUEUE_ADDR(lid) (HDR_ADDR(lid) + sizeof(lmeta))
#define LQE_ADDR(lid, slot) (LQUEUE_ADDR(lid) + (slot) * sizeof(lqe))

#define MN_LOCK_MR_SIZE(lock_num) ((lock_num) * sizeof(aql))

/**
 * Special values for header.
 */
#define CQL_RELEASED 0xffffbeef

extern "C" {
int dmlock_setup_lm(int lknum, char* mn_buf);
int dmlock_setup_mn(int lock_num, int mr_size);
}

#endif
