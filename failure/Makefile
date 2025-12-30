
BUILD_DIR := ../build

COORDINATOR_BIN := $(BUILD_DIR)/coordinator
REPORTER_BIN := $(BUILD_DIR)/report_failure

CFLAGS := -I../include

all: $(COORDINATOR_BIN) $(REPORTER_BIN)

$(COORDINATOR_BIN): coordinator.cc
	$(CXX) -std=c++17 $< ../lib/conf.cc.o -o $@ $(CFLAGS)

$(REPORTER_BIN): reporter.cc
	$(CXX) -std=c++17 $< ../lib/conf.cc.o -o $@ $(CFLAGS)
