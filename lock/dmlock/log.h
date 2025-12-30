
#ifndef __DMLOCK_LOG_H
#define __DMLOCK_LOG_H

#include "debug.h"

#ifdef DMLOCK_DEBUG
#define LC_DEBUG(fmt, ...) \
  LLOG("[host%d][core%d][crt%d][lock%d]" fmt, LHID, \
    TID_CORE(tid), TID_CRT(tid), lock, ##__VA_ARGS__)
#else
#define LC_DEBUG(fmt, ...) (0)
#endif

#define LC_LOG(fmt, ...) \
  LLOG("[host%d][core%d][crt%d][lock%d]" fmt, LHID, \
    TID_CORE(tid), TID_CRT(tid), lock, ##__VA_ARGS__)

#endif