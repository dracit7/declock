
TESTS_UTIL_DIR := $(TESTS_DIR)/util

TESTS_SRC := $(shell find $(TESTS_DIR) -maxdepth 1 -name "*.cc")
TESTS := $(patsubst $(TESTS_DIR)/%.cc, $(BUILD_DIR)/$(SYSTEM)_%, $(TESTS_SRC))

$(BUILD_DIR)/$(SYSTEM)_microbench: $(TESTS_DIR)/microbench.cc \
							$(TESTS_UTIL_SRC) $(R2_OBJ) $(BOOST_OBJ)\
							$(LIB_OBJ) $(LOCK_OBJ) $(EXTRA_OBJ) $(JUNCTION_OBJ)
	@mkdir -p $(BUILD_DIR)
	$(CXX) $(CFLAGS) -I$(TESTS_UTIL_DIR) $^ $(LDFLAGS) -o $@

microbench: $(BUILD_DIR)/$(SYSTEM)_microbench
