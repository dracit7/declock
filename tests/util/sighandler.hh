#include <signal.h>

#include "debug.h"
#include "conf.h"

/**
 * The signal handler.
 */
void signal_handler(int signum, siginfo_t* info, void* context) {
  LLOG("[host%u]Exited", LHID);
  fflush(stderr);
  fflush(stdout);
  exit(0);
}

void setup_sighandler() {
  struct sigaction act, old_action;
  act.sa_sigaction = signal_handler;
  act.sa_flags = SA_SIGINFO;
  sigemptyset(&(act.sa_mask));
  sigaction(SIGINT, &act, &old_action);
}