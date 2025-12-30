
#include <cstring>
#include <unordered_map>
#include <set>

#include "../lib/conf.h"
#include "../lib/util.h"
#include "../vendor/rib/core/lib.hh"
#include "../vendor/rib/core/rctrl.hh"

using namespace rdmaio;
using std::unordered_map;
using std::set;

set<uint32_t> surviving_servers;

class rpc_server {

  // The RPC server states and metadata.
  std::atomic<bool> running;
  pthread_t handler_tid;
  bootstrap::SRpcHandler rpc;

public:
  explicit rpc_server(const usize &port)
      : running(false), rpc(port, "0.0.0.0") {

    RDMA_ASSERT(
      rpc.register_handler(RPC_SERVER_JOIN,
        std::bind(&rpc_server::server_join, this, std::placeholders::_1))
    );

    RDMA_ASSERT(
      rpc.register_handler(RPC_REPORT_SERVER_FAILURE,
        std::bind(&rpc_server::report_server_failure, 
            this, std::placeholders::_1))
    );

    RDMA_ASSERT(
      rpc.register_handler(RPC_GET_SURVIVING_SERVERS,
        std::bind(&rpc_server::get_surviving_servers, 
            this, std::placeholders::_1))
    );

  }
 
  ~rpc_server() {
    this->stop_daemon();
  }

  bool start_daemon() {
    running = true;
    asm volatile("" ::: "memory");

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    return (pthread_create(&handler_tid, &attr, &rpc_server::daemon, this) == 0);
  }

  void stop_daemon() {
    if (running) {
      running = false;
      asm volatile("" ::: "memory");
      pthread_join(handler_tid, nullptr);
    }
  }

  bool run_daemon() {
    running = true;
    asm volatile("" ::: "memory");
    daemon(this);
    return true;
  }

  static void *daemon(void *ctx) {
    rpc_server &ctrl = *((rpc_server *)ctx);
    uint64_t total_reqs = 0;
    while (ctrl.running) {
      total_reqs += ctrl.rpc.run_one_event_loop();
      continue;
    }
    RDMA_LOG(INFO) << "stop with :" << total_reqs << " processed.";
    return nullptr;
  }

private:

  ByteBuffer server_join(const ByteBuffer &b) {
    auto arg = ::rdmaio::Marshal::dedump<sid_req>(b);
    if (arg) {
      u64 srv_id = arg.value().server_id;
      surviving_servers.insert(srv_id);
      printf("cn %d joined\n", srv_id);
    }

    return ::rdmaio::Marshal::dump_null<int>();
  }

  ByteBuffer report_server_failure(const ByteBuffer &b) {
    auto arg = ::rdmaio::Marshal::dedump<sid_req>(b);
    if (arg) {
      u64 srv_id = arg.value().server_id;
      surviving_servers.erase(srv_id);
      printf("reported cn %d failure\n", srv_id);
    }

    return ::rdmaio::Marshal::dump_null<int>();
  }

  ByteBuffer get_surviving_servers(const ByteBuffer &b) {
    servers_reply srv_reply;
    srv_reply.server_num = 0;
    for (auto sid: surviving_servers) 
      srv_reply.server_ids[srv_reply.server_num++] = sid;

    return ::rdmaio::Marshal::dump<servers_reply>(srv_reply);
  }
};

rpc_server* rpc_srv;

int main() {
  rpc_srv = new rpc_server(COORDINATOR_PORT);
  rpc_srv->run_daemon();
  return 0;
}