#include <thread>

#include "lcore.h"

thread_local uint16_t lcore_id = 0;

/**
 * set_lcore_id: Record current thread's
 * logic core id in the thread-local variable.
*/
void set_lcore_id(uint16_t cid) {
  lcore_id = cid;
}

/**
 * get_cur_lcore_id(): Read current thread's
 * logic core id from the thread-local variable.
*/
uint16_t get_cur_lcore_id() {
  return lcore_id;
}

