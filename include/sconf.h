
/**
 * Configurations about the machine.
 */
#define MAX_CN_NUM        32
#define MAX_MN_NUM        8
#define MAX_CORE_NUM      16

/**
 * Use the asynchronous APIs or not.
 */
#if !defined(APP_SHERMAN) && !defined (APP_FORD)
#define ASYNC_API
#endif

#define MAX_CRT_NUM 8
#define MAX_CLIENT_NUM (MAX_CORE_NUM * MAX_CRT_NUM)

/**
 * Configurations about RDMA.
 * 
 * Each server runs an RCtrl instance for hosting the QP attributes
 * on the server. Other servers set up RC QPs or acquire UD QP attrs
 * via the RPC server (RCtrl).
 */
#define RIB_RCTRL_PORT_BASE 17880
#define RIB_RCTRL_PORT(mid) (RIB_RCTRL_PORT_BASE + mid)

// The memory region size for SEND and RECV of each QP.
#define RDMA_RECV_MR_SIZE (1024 * 1024)
#define RDMA_SEND_MR_SIZE (512)
#define RDMA_CLIENT_RC_MR_SIZE ((uint64_t)1024 * 1024)
#define RDMA_CN_RC_MR_SIZE (RDMA_CLIENT_RC_MR_SIZE * MAX_CRT_NUM)

// The size of each RECV entry and the buffer length.
#define RDMA_RECV_ENTRY_SIZE (RDMA_SEND_MR_SIZE)
#define RDMA_RECV_ENTRY_NUM  (2048) /* Maximum size of RECV entries */

// The key of RC MR and NIC on memory nodes.
#define RDMA_RC_MR_KEY 73
#define RDMA_NIC_KEY   73

// The max batch size of RDMA verbs.
#define RDMA_MAX_BATCH_SIZE 16

/**
 * Environment variable names for dynamic configurations.
 */
#define ENV_MACHINE_ID  "MACHINE_ID"
#define ENV_NUMA_ID     "NUMA_ID"
#define ENV_MACHINE_IPS "MACHINE_IPS"
#define ENV_QUEUE_CAP   "QUEUE_CAPACITY"
#define ENV_CORE_NUM    "CORE_NUM"
#define ENV_CRT_NUM     "COROUTINE_NUM"

/**
 * Port used by the coordinator service.
 */
// #define USE_COORDINATOR
#define COORDINATOR_PORT 10287

/**
 * Timeout for acquisition (in ms).
 */
#define ACQUIRE_TIMEOUT 50
