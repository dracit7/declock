
LIB_SRC := $(shell find $(LIB_DIR) -name "*.c")
LIB_CPP_SRC := $(shell find $(LIB_DIR) -name "*.cc")
LIB_OBJ := $(patsubst %.c, %.c.o, $(LIB_SRC)) \
					 $(patsubst %.cc, %.cc.o, $(LIB_CPP_SRC)) \

$(LIB_DIR)/%.c.o: $(LIB_DIR)/%.c $(LIB_DIR)/%.h $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB_DIR)/%.cc.o: $(LIB_DIR)/%.cc $(LIB_DIR)/%.h $(HEADERS)
	$(CXX) $(CFLAGS) -c $< -o $@