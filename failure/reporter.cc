
#include <cstdio>
#include <string>
#include <sstream>

#include "../lib/conf.h"
#include "../lib/util.h"
#include "../vendor/rib/core/lib.hh"

using namespace rdmaio::bootstrap;
using namespace std;

int main(int argc, const char* argv[]) {
  if (argc < 2) {
    printf("usage: reporter <coordinator_ip> <failed_cn_id>\n");
    exit(1);
  }

  stringstream ss;
  ss << argv[1] << ":" << COORDINATOR_PORT;

  SRpc* rpc_client = new SRpc(ss.str());
  int failed_mid = atoi(argv[2]);

  sid_req req = {.server_id = failed_mid};

  auto res = rpc_client->call(RPC_REPORT_SERVER_FAILURE, 
    rdmaio::ByteBuffer((const char*)&req, sizeof(sid_req)));
  auto res_reply = rpc_client->receive_reply();
  return 0;
}
