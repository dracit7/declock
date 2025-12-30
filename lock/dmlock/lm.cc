
#include "conf.h"
#include "rdma.h"
#include "lm.h"
#include "debug.h"

static int lock_num;

/**
 * Does not need to be called when DMLock objects are integrated 
 * in data items.
 */
int dmlock_setup_lm(int lknum, char* mn_buf) {
  lock_num = lknum;

  aql* glob_lk_tab = (aql *)mn_buf;
  for (int i = 0; i < lock_num; i++) {
    glob_lk_tab[i].hdr = 0;
    for (int j = 0; j < MAX_QUEUE_SIZE; j++) {
      glob_lk_tab[i].queue[j].ver = -1;
    }
  }

  LLOG("[host%u]DMLock set up lock manager, lock_num: %d", LHID, lock_num);
  return 0;
}

int dmlock_setup_mn(int lock_num, int mr_size) {
  get_conf_from_env();
  rdma_setup_mn(NUMA_ID, mr_size);
  return dmlock_setup_lm(lock_num, rdma_mn_buf());
}
