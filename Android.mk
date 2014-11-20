LOCAL_PATH := $(call my-dir)

include $(CLEAR_VARS)

LOCAL_MODULE := rocketspeed

LOCAL_SRC_FILES := \
	src/client/client.cc \
	src/client/client_env.cc \
	src/messages/event_loop.cc \
	src/messages/messages.cc \
	src/messages/msg_loop.cc \
	src/util/build_version.cc \
	src/util/common/coding.cc \
	src/util/common/guid_generator.cc \
	src/util/common/statistics.cc \

LOCAL_C_INCLUDES += $(LOCAL_PATH)

LOCAL_EXPORT_C_INCLUDES := $(LOCAL_C_INCLUDES)

LOCAL_CPPFLAGS := \
	-std=c++11 \
	-Wall \
	-Werror \
	-DROCKETSPEED_PLATFORM_ANDROID \
	-DOS_ANDROID \
	-DUSE_UPSTREAM_LIBEVENT \

LOCAL_CPP_FEATURES := exceptions

LOCAL_STATIC_LIBRARIES := libevent2_static

include $(BUILD_STATIC_LIBRARY)

$(call import-module,libevent-2.0.21)
