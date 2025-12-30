
LOCK_SRC := $(shell find $(LOCK_DIR) -name "*.c")
LOCK_CPP_SRC := $(shell find $(LOCK_DIR) -name "*.cc")
LOCK_OBJ := $(patsubst %.c, %.c.o, $(LOCK_SRC)) \
					 $(patsubst %.cc, %.cc.o, $(LOCK_CPP_SRC)) \

$(LOCK_DIR)/%.c.o: $(LOCK_DIR)/%.c $(LOCK_DIR)/%.h $(HEADERS) \
								  	$(LOCK_DIR)/common.h $(LOCK_DIR)/lm.h
	$(CC) $(CFLAGS) -c $< -o $@

$(LOCK_DIR)/%.cc.o: $(LOCK_DIR)/%.cc $(LOCK_DIR)/%.h $(HEADERS) \
								  	$(LOCK_DIR)/common.h $(LOCK_DIR)/lm.h
	$(CXX) -std=c++17 $(CFLAGS) -c $< -o $@