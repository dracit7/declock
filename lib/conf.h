
#ifndef __DMLOCK_CONF_H
#define __DMLOCK_CONF_H

#include <vector>
#include <cstdint>

#include "sconf.h"

typedef struct {
  uint16_t localhost_id;
  uint8_t  numa_id; 
  uint32_t lock_queue_capacity;

  uint32_t core_num;
  uint32_t crt_num;

  /* IP of machines in the cluster */
  std::vector<char*> machine_ip; 
  std::vector<uint16_t> cns; 
  std::vector<uint16_t> mns; 
} dmlock_conf;

extern dmlock_conf confs;

#define LHID        (confs.localhost_id)
#define NUMA_ID     (confs.numa_id)
#define LQ_CAPACITY (confs.lock_queue_capacity)
#define CORE_NUM    (confs.core_num)
#define CRT_NUM     (confs.crt_num)
#define MACHINES    (confs.machine_ip)
#define CN_LIST     (confs.cns)
#define MN_LIST     (confs.mns)

extern "C" {
void get_conf_from_env();
int get_machine_num();
}

#endif