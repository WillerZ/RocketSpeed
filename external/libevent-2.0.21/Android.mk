LOCAL_PATH := $(call my-dir)

# Sources and compilation options
LIBEVENT_MODULE := libevent2

LIBEVENT_SRC_FILES:= \
	buffer.c \
	bufferevent.c \
	bufferevent_filter.c \
	bufferevent_pair.c \
	bufferevent_ratelim.c \
	bufferevent_sock.c \
	epoll.c \
	epoll_sub.c \
	evdns.c \
	event.c \
	event_tagging.c \
	evmap.c \
	evrpc.c \
	evthread.c \
	evthread_pthread.c \
	evutil.c \
	evutil_rand.c \
	http.c \
	listener.c \
	log.c \
	poll.c \
	select.c \
	signal.c \
	strlcpy.c
#	The following sources are not Android compatible: \
	arc4random.c \
	buffer_iocp.c \
	bufferevent_async.c \
	bufferevent_openssl.c \
	devpoll.c \
	event_iocp.c \
	evport.c \
	evthread_win32.c \
	kqueue.c \
	log.c \

LIBEVENT_C_INCLUDES :=
LIBEVENT_C_INCLUDES += $(LOCAL_PATH)/android
LIBEVENT_C_INCLUDES += $(LOCAL_PATH)/include

LIBEVENT_EXPORT_C_INCLUDES := $(LIBEVENT_C_INCLUDES)

LIBEVENT_CFLAGS := -DHAVE_CONFIG_H -DOS_ANDROID

# A production release have most symbols as hidden
# so that the size of the resulting code size is reduced.
# https://gcc.gnu.org/onlinedocs/gcc-4.4.2/gcc/Code-Gen-Options.html
# For test build, we use default visibility because the
# unit tests use internal libevent functions
ifeq ($(LIBEVENT_RELEASE_BUILD),true)
  LIBEVENT_CFLAGS += -fvisibility=hidden
else
  LIBEVENT_CFLAGS += -O0 -ggdb -g -gdwarf-2
endif

# Build as shared library
include $(CLEAR_VARS)

LOCAL_MODULE := $(LIBEVENT_MODULE)_shared
LOCAL_SRC_FILES := $(LIBEVENT_SRC_FILES)
LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)

include $(BUILD_SHARED_LIBRARY)

# Build as static library
include $(CLEAR_VARS)

LOCAL_MODULE := $(LIBEVENT_MODULE)_static
LOCAL_SRC_FILES := $(LIBEVENT_SRC_FILES)
LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)

include $(BUILD_STATIC_LIBRARY)

# if we are not doing a release-build, then build unit tests.
ifeq ($(LIBEVENT_RELEASE_BUILD),false)

  include $(CLEAR_VARS)
  LOCAL_MODULE := test-init
  LOCAL_SRC_FILES := test/test-init.c
  LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
  LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
  LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)
  LOCAL_SHARED_LIBRARIES := libevent2_shared
  include $(BUILD_EXECUTABLE)
  
  include $(CLEAR_VARS)
  LOCAL_MODULE := bench
  LOCAL_SRC_FILES := test/bench.c
  LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
  LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
  LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)
  LOCAL_SHARED_LIBRARIES := libevent2_shared
  include $(BUILD_EXECUTABLE)
  
  include $(CLEAR_VARS)
  LOCAL_MODULE := test-ratelim
  LOCAL_SRC_FILES := test/test-ratelim.c
  LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
  LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
  LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)
  LOCAL_SHARED_LIBRARIES := libevent2_shared
  include $(BUILD_EXECUTABLE)
  
  include $(CLEAR_VARS)
  LOCAL_MODULE := test-time
  LOCAL_SRC_FILES := test/test-time.c
  LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
  LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
  LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)
  LOCAL_SHARED_LIBRARIES := libevent2_shared
  include $(BUILD_EXECUTABLE)
  
  include $(CLEAR_VARS)
  LOCAL_MODULE := test-eof
  LOCAL_SRC_FILES := test/test-eof.c
  LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
  LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
  LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)
  LOCAL_SHARED_LIBRARIES := libevent2_shared
  include $(BUILD_EXECUTABLE)
  
  include $(CLEAR_VARS)
  LOCAL_MODULE := test-weof
  LOCAL_SRC_FILES := test/test-weof.c
  LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
  LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
  LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)
  LOCAL_SHARED_LIBRARIES := libevent2_shared
  include $(BUILD_EXECUTABLE)
  
  include $(CLEAR_VARS)
  LOCAL_MODULE := test-changelist
  LOCAL_SRC_FILES := test/test-changelist.c
  LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
  LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
  LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)
  LOCAL_SHARED_LIBRARIES := libevent2_shared
  include $(BUILD_EXECUTABLE)
  
  include $(CLEAR_VARS)
  LOCAL_MODULE := test-regress
  LOCAL_SRC_FILES := test/regress_main.c test/regress.c test/tinytest.c \
                     test/regress_minheap.c test/regress_et.c test/regress_buffer.c \
                     test/regress_util.c test/regress_bufferevent.c \
                     test/regress_http.c test/regress_dns.c test/regress_rpc.c \
                     test/regress_thread.c test/regress_testutils.c \
                     test/regress_listener.c test/regress.gen.c
  LOCAL_C_INCLUDES := $(LIBEVENT_C_INCLUDES)
  LOCAL_EXPORT_C_INCLUDES := $(LIBEVENT_EXPORT_C_INCLUDES)
  LOCAL_CFLAGS := $(LIBEVENT_CFLAGS)
  LOCAL_SHARED_LIBRARIES := libevent2_shared
  include $(BUILD_EXECUTABLE)
  
endif

