
#include <chrono>
#include <string>
#include <sstream>

using namespace std;
using namespace chrono;

#include "util.h"
#include "conf.h"
#include "lcore.h"

#include "core/lib.hh"

rdmaio::SRpc* rpc_client;

static steady_clock::time_point start_timestamp;
static bool started = false;

void delay(uint64_t nanosec) {
  const auto timeout = nanoseconds(nanosec);
  auto start = steady_clock::now();
  while(steady_clock::now() - start < timeout) ;
}

void set_start_timestamp() {
  start_timestamp = steady_clock::now();
  started = true;
}

void wait_until_start() {
  while (!started) ;
}

uint32_t timestamp_now(uint16_t core) {
  return duration_cast<microseconds>(
    steady_clock::now() - start_timestamp).count();
}

void connect_coordinator() {
  stringstream ss;
  ss << MACHINES[0] << ":" << COORDINATOR_PORT;
  rpc_client = new rdmaio::SRpc(ss.str());
}

void cn_join(uint32_t mid) {
  sid_req req = {.server_id = mid};
  auto res = rpc_client->call(RPC_SERVER_JOIN, 
    rdmaio::ByteBuffer((const char*)&req, sizeof(sid_req)));
  auto res_reply = rpc_client->receive_reply();
}

void cn_exit(uint32_t mid) {
#ifdef USE_COORDINATOR
  sid_req req = {.server_id = mid};
  auto res = rpc_client->call(RPC_REPORT_SERVER_FAILURE, 
    rdmaio::ByteBuffer((const char*)&req, sizeof(sid_req)));
  auto res_reply = rpc_client->receive_reply();
#endif
}

int get_surviving_cns(uint32_t* cns) {
  auto res = rpc_client->call(RPC_GET_SURVIVING_SERVERS, rdmaio::ByteBuffer());
  auto res_reply = rpc_client->receive_reply();

  auto reply = (servers_reply *)res_reply.desc.data();
  for (int i = 0; i < reply->server_num; i++) cns[i] = reply->server_ids[i];
  return reply->server_num;
}