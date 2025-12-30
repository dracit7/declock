
#ifndef __DMLOCK_COMMON_H
#define __DMLOCK_COMMON_H

#include "async_adaptor.h"

/* Configurations */
#ifndef CQL_ONLY
#define USE_COHORT

#ifndef NO_TIMESTAMP
#define USE_TIMESTAMP
#endif
#endif

#endif