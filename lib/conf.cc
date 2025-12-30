
#include <stdlib.h>
#include <string.h>

#include "debug.h"
#include "conf.h"

dmlock_conf confs = {
  .localhost_id = 0,
  .numa_id = 0,
  .lock_queue_capacity = 0,
  .core_num = 1,
  .crt_num = 1,
};

void get_conf_from_env() {

  // Get local host ID.
  auto lhid_raw = getenv(ENV_MACHINE_ID);
  ERROR_IF(!lhid_raw, ENV_MACHINE_ID " not set");
  confs.localhost_id = atoi(lhid_raw);

  // Get NUMA ID.
  auto numa_id_raw = getenv(ENV_NUMA_ID);
  ERROR_IF(!numa_id_raw, ENV_NUMA_ID " not set");
  confs.numa_id = atoi(numa_id_raw);

  // Get the cluster information.
  // 
  // Cluster information format: "xx.xx.xxx.xx;xxx.xx.xx.x;...",
  // which should follow the order of host IDs.
  auto iplist_raw = getenv(ENV_MACHINE_IPS);
  ERROR_IF(!iplist_raw, ENV_MACHINE_IPS " not set");
  char * iplist_copy = strdup(iplist_raw);

  auto token = strtok(iplist_copy, ";");
  while (token) {
    bool is_mn = (token[0] == 'c') ? false : true;
    if (is_mn) confs.mns.push_back(confs.machine_ip.size());
    else confs.cns.push_back(confs.machine_ip.size());
    confs.machine_ip.push_back(strdup(&token[3]));
    token = strtok(NULL, ";");
  }

  // Get the lock queue capacity.
  auto qcap = getenv(ENV_QUEUE_CAP);
  if (qcap && atoi(qcap)) confs.lock_queue_capacity = atoi(qcap);
  else confs.lock_queue_capacity = 0;

  // Get the core num and coroutine num.
  auto cores = getenv(ENV_CORE_NUM);
  if (cores) confs.core_num = atoi(cores);
  auto crts = getenv(ENV_CRT_NUM);
  if (crts) confs.crt_num = atoi(crts);
}

int get_machine_num() {
  return confs.machine_ip.size();
}
