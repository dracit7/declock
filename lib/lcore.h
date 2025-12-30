#ifndef __DMLOCK_LCORE_H
#define __DMLOCK_LCORE_H

#include <thread>
#include <cstdint>

/**
 * The logic core module.
 * 
 * This module uses a thread-local variable 
 * to record the logic core id of a thread,
 * so that current thread can get its logic
 * core id at its runtime.
 */

void set_lcore_id(uint16_t cid);
uint16_t get_cur_lcore_id();

#endif