
.DEFAULT_GOAL := all

BUILD_DIR := ./build
LIB_DIR := ./lib
INC_DIR := ./include
TESTS_DIR := ./tests

SYSTEM ?= dmlock
ifeq ($(SYSTEM), dmlock)
LOCK_DIR := ./lock/dmlock
else ifeq ($(SYSTEM), cqlock)
LOCK_DIR := ./lock/dmlock
CFLAGS += -DCQL_ONLY
else ifeq ($(SYSTEM), chtlock)
LOCK_DIR := ./lock/dmlock
CFLAGS += -DNO_TIMESTAMP
else ifeq ($(SYSTEM), dmlock_tf)
LOCK_DIR := ./lock/dmlock
CFLAGS += -DTASK_FAIR
else ifeq ($(SYSTEM), dslr)
LOCK_DIR := ./lock/dslr
else ifeq ($(SYSTEM), caslock)
LOCK_DIR := ./lock/caslock
endif

APPLICATION ?= null
ifneq ($(APPLICATION), null)
EXTRA_SRC := lock/dmlock/remote_$(APPLICATION).cpp
EXTRA_OBJ := lock/dmlock/remote_$(APPLICATION).cpp.o
endif

ASYNC_BACKEND ?= r2
ifneq ($(APPLICATION), null)
ASYNC_BACKEND := null
endif
CFLAGS += -Iasync/$(ASYNC_BACKEND)/

ifeq ($(APPLICATION), ford)
APP_INC_DIR := -Iapp/$(APPLICATION)/core
APP_INC_DIR += -Iapp/$(APPLICATION)/workload
APP_INC_DIR += -Iapp/$(APPLICATION)/thirdparty
CFLAGS += -DAPP_FORD
else ifeq ($(APPLICATION), sherman)
APP_INC_DIR := -Iapp/$(APPLICATION)/include
CFLAGS += -DAPP_SHERMAN
endif

RIB_DIR := ./vendor/rib
R2_DIR := ./vendor/r2

R2_DEPS_DIR := $(R2_DIR)/deps
R2_SRC_DIR := $(R2_DIR)/src
BOOST_INC_DIR := $(R2_DEPS_DIR)/boost/include
BOOST_LIB_DIR := $(R2_DEPS_DIR)/boost/lib

R2_OBJ := $(R2_DIR)/libr2.a
BOOST_OBJ := $(BOOST_LIB_DIR)/libboost_coroutine.a
BOOST_OBJ += $(BOOST_LIB_DIR)/libboost_thread.a
BOOST_OBJ += $(BOOST_LIB_DIR)/libboost_context.a
BOOST_OBJ += $(BOOST_LIB_DIR)/libboost_system.a

JUNCTION_INC_DIR := ./vendor/junction/install/include
JUNCTION_LIB_DIR := ./vendor/junction/install/lib
JUNCTION_OBJ := $(JUNCTION_LIB_DIR)/libjunction.a $(JUNCTION_LIB_DIR)/libturf.a 

DEBUG ?= 1

CC ?= gcc
CFLAGS += -I$(INC_DIR) -I$(LIB_DIR) -I$(LOCK_DIR) -I$(RIB_DIR)
CFLAGS += -I$(JUNCTION_INC_DIR)
ifeq ($(APPLICATION), null)
CFLAGS += -I$(R2_DEPS_DIR)
CFLAGS += -I$(R2_SRC_DIR)
CFLAGS += -I$(BOOST_INC_DIR)
endif
CFLAGS += $(APP_INC_DIR)
CFLAGS += -fPIC
CFLAGS += -DNO_DCT -std=c++17 
LDFLAGS += -libverbs -lpthread

ifeq ($(DEBUG), 1)
	CFLAGS += -g
endif

include $(INC_DIR)/Makefile
include $(LIB_DIR)/Makefile
include $(LOCK_DIR)/Makefile
.PRECIOUS: $(LIB_OBJ) $(LOCK_OBJ)

makearchive = "create $(1)\n $(foreach mod,$(2),addmod $(mod)\n) $(foreach lib,$(3),addlib $(lib)\n) save\nend\n"

DMLOCK_STATIC_LIB := $(BUILD_DIR)/libdmlock_$(APPLICATION).a

$(DMLOCK_STATIC_LIB): $(LIB_OBJ) $(LOCK_OBJ) $(EXTRA_SRC) lock/dmlock/common.h
	@mkdir -p $(BUILD_DIR)
	@if [ ! -z "$(EXTRA_OBJ)" ]; then \
		$(CXX) -std=c++17 -c $(CFLAGS) $(EXTRA_SRC) -o $(EXTRA_OBJ); \
	fi
	echo $(call makearchive,$@,$(LIB_OBJ) $(LOCK_OBJ) $(EXTRA_OBJ), $(JUNCTION_OBJ)) | ar -M

static_lib: $(DMLOCK_STATIC_LIB)

include $(TESTS_DIR)/Makefile

all: $(TESTS)

clean:
	rm -rf $(LIB_OBJ) $(TESTS) $(LOCK_OBJ) $(DMLOCK_STATIC_LIB)

clean-lib:
	rm -f $(LIB_OBJ) $(LOCK_OBJ)
