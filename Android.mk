LOCAL_PATH := $(call my-dir)

ROCKETSPEED_CFLAGS := \
	-std=c++11 \
	-Wall \
	-Werror \
	-DGFLAGS=gflags \
        -DROCKETSPEED_PLATFORM_POSIX \
	-DOS_ANDROID \
	-DUSE_UPSTREAM_LIBEVENT

ifeq ($(ROCKETSPEED_RELEASE_BUILD),true)
  ROCKETSPEED_CFLAGS += -fvisibility=hidden -DNDEBUG
else
  ROCKETSPEED_CFLAGS += -O0 -ggdb -g -gdwarf-2 -UNDEBUG
endif

include $(CLEAR_VARS)

LOCAL_MODULE := rocketspeed

LOCAL_SRC_FILES := \
       src/client/client.cc \
       src/client/client_env.cc \
       src/messages/event_loop.cc \
       src/messages/messages.cc \
       src/messages/msg_loop.cc \
       src/port/port_android.cc \
       src/util/build_version.cc \
       src/util/common/guid_generator.cc \
       src/util/common/coding.cc \
       src/util/common/statistics.cc \
       src/util/common/base_env.cc \
       src/util/common/status.cc \
       src/util/common/host.cc

LOCAL_C_INCLUDES += $(LOCAL_PATH) $(LOCAL_PATH)/include

LOCAL_EXPORT_C_INCLUDES := $(LOCAL_C_INCLUDES)

LOCAL_CPPFLAGS := $(ROCKETSPEED_CFLAGS)

LOCAL_CPP_FEATURES := exceptions

LOCAL_STATIC_LIBRARIES := libevent2_static

include $(BUILD_SHARED_LIBRARY)

# If we are not doing a release-build, then build unit tests,
# debugging utilities, benchmark binaries, etc

ifeq ($(ROCKETSPEED_RELEASE_BUILD),false)

include $(CLEAR_VARS)
LOCAL_MODULE := rocketbench

LOCAL_SRC_FILES := \
       src/util/auto_roll_logger.cc \
       src/util/parsing.cc \
       src/tools/rocketbench/random_distribution.cc \
       src/port/port_posix.cc \
       src/util/env_posix.cc \
       src/util/env.cc \
       external/gflags/src/gflags.cc \
       external/gflags/src/gflags_completions.cc \
       external/gflags/src/gflags_reporting.cc \
       src/tools/rocketbench/main.cc \

LOCAL_C_INCLUDES += $(LOCAL_PATH)/external/gflags/include \
                    $(LOCAL_PATH)/external/gflags/include/gflags \
                    $(LOCAL_PATH) $(LOCAL_PATH)/include

LOCAL_EXPORT_C_INCLUDES := $(LOCAL_C_INCLUDES)

LOCAL_CPPFLAGS := $(ROCKETSPEED_CFLAGS)

# The binary depends on the rocketspeed library
LOCAL_SHARED_LIBRARIES := rocketspeed

include $(BUILD_EXECUTABLE)

endif

$(call import-module,libevent-2.0.21)

