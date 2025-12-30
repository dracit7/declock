
#ifndef __DMLOCK_DEBUG_H
#define __DMLOCK_DEBUG_H

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

// #define DMLOCK_DEBUG

typedef enum {
  ERR_NOLOCK = 1,
  ERR_LOCK_DOUBLE_FREE,
  ERR_INVALID_OP,
} error_code;

#define ERROR(fmt, ...) do {\
  fprintf(stderr, "[Error]" fmt "\n", ##__VA_ARGS__);\
  exit(1);\
} while (0)

#define EXCEPTION(fmt, ...) do {\
  fprintf(stderr, "[Exception] <%s:%d> " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__);\
  exit(1);\
} while (0)

#define ERROR_IF(cond, msg, ...) do {\
  if (cond) {\
    fprintf(stderr, "[Error] <%s:%d> " msg "\n",\
      __FILE__, __LINE__, ##__VA_ARGS__);\
    exit(1);\
  }\
} while (0)

#define LLOG(fmt, ...) do {\
  fprintf(stderr, "[Info]" fmt "\n", ##__VA_ARGS__);\
  fflush(stderr);\
} while (0)

#ifdef DMLOCK_DEBUG

#define LASSERT(cond) do {\
  if (!(cond)) {\
    fprintf(stderr, "[Assertion failed] <%s:%d> %s\n",\
      __FILE__, __LINE__, #cond);\
    exit(1);\
  }\
} while (0)

#define ASSERT_MSG(cond, msg, ...) do {\
  if (!(cond)) {\
    fprintf(stderr, "[Assertion failed] <%s:%d> " msg "\n",\
      __FILE__, __LINE__, ##__VA_ARGS__);\
    exit(1);\
  }\
} while (0)

#define DEBUG(fmt, ...) do {\
  fprintf(stderr, "[Debug]" fmt "\n", ##__VA_ARGS__);\
  fflush(stderr);\
} while (0)

#else

#define DEBUG(fmt, ...) (0)
#define LASSERT(cond) (cond)
#define ASSERT_MSG(cond, msg, ...) (cond)

#endif

#endif
