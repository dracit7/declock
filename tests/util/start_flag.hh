
#include <stdint.h>

#include "rdma.h"
#include "sconf.h"
#include "lcore.h"
#include "debug.h"

#define MN_FLAG_SIZE ((uint64_t)64)
#define FLAG_START_ADDR (MN_MR_SIZE - MN_FLAG_SIZE)

static void cn_sync_time(uint64_t addr, uint16_t mn_id, uint32_t cn_num) {
  uint16_t used_core = get_cur_lcore_id();
  uint64_t* val = (uint64_t *)rdma_rc_buf(used_core);
  rdma_fa(used_core, mn_id, (char*)val, addr, 1, true);
  LLOG("Returned flag value: %ld, machine_num: %d", *val, cn_num);

  if (*val == cn_num - 1) {
    *val = 0;
    rdma_write(used_core, mn_id, (char*)val, addr, sizeof(uint64_t), true);
    set_start_timestamp();
    return;
  }

  do {
    rdma_read(used_core, mn_id, (char*)val, addr, sizeof(uint64_t), true);
  } while (*val);

  set_start_timestamp();
}
