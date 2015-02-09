# A macro which builds base rocketspeed library.
def rocketspeed_cxx_library(name, extra_compiler_flags):
  cxx_library(
    name = name,
    srcs = [
      'src/client/client.cc',
      'src/client/client_env.cc',
      'src/client/options.cc',
      'src/client/storage/file_storage.cc',
      'src/messages/descriptor_event.cc',
      'src/messages/event_loop.cc',
      'src/messages/messages.cc',
      'src/messages/msg_loop.cc',
      'src/port/port_android.cc',
      'src/util/build_version.cc',
      'src/util/common/base_env.cc',
      'src/util/common/coding.cc',
      'src/util/common/configuration.cc',
      'src/util/common/guid_generator.cc',
      'src/util/common/host.cc',
      'src/util/common/statistics.cc',
      'src/util/common/status.cc',
    ],
    header_namespace = '',
    headers = subdir_glob([
      ('', 'include/*.h'),
      ('', 'src/**/*.h'),
      ('', 'external/folly/*.h'),
    ]),
    preprocessor_flags = [
      '-DUSE_UPSTREAM_LIBEVENT',
    ],
    exported_preprocessor_flags = [
      '-DROCKETSPEED_PLATFORM_POSIX',
      '-DOS_ANDROID',
    ],
    compiler_flags = [
      '-std=c++11',
      '-fno-omit-frame-pointer',
      '-fexceptions',
      '-Wall',
      '-Werror',
    ] + extra_compiler_flags,
    deps = [
      '//native/third-party/libevent-2.0.21:libevent-2.0.21',
    ],
    visibility = [
      '//native/third-party/rocketspeed/...',
    ],
  )

# Rocketspeed C++ library.
rocketspeed_cxx_library(
  name = 'rocketspeed',
  extra_compiler_flags = [
    '-fvisibility=hidden',
    '-DNDEBUG',
  ],
)

# Rocketspeed C++ library -- debug build.
rocketspeed_cxx_library(
  name = 'rocketspeed_debug',
  extra_compiler_flags = [
    '-O0',
    '-ggdb',
    '-g',
    '-gdwarf-2',
    '-UNDEBUG',
  ],
)

# Rocketspeed C++ JNI bindings.
cxx_library(
  name = 'rocketspeedjni',
  srcs = [
    'src/djinni/client.cc',
    'src/djinni/djinni_main.cc',
    'src/djinni/jvm_env.cc',
    'src-gen/djinni/HostId.cpp',
    'src-gen/djinni/MsgIdImpl.cpp',
    'src-gen/djinni/jni/NativeClientImpl.cpp',
    'src-gen/djinni/jni/NativeConfigurationImpl.cpp',
    'src-gen/djinni/jni/NativeHostId.cpp',
    'src-gen/djinni/jni/NativeMsgIdImpl.cpp',
    'src-gen/djinni/jni/NativePublishCallbackImpl.cpp',
    'src-gen/djinni/jni/NativePublishStatus.cpp',
    'src-gen/djinni/jni/NativeReceiveCallbackImpl.cpp',
    'src-gen/djinni/jni/NativeSnapshotCallbackImpl.cpp',
    'src-gen/djinni/jni/NativeStatus.cpp',
    'src-gen/djinni/jni/NativeSubscribeCallbackImpl.cpp',
    'src-gen/djinni/jni/NativeSubscriptionRequestImpl.cpp',
    'src-gen/djinni/jni/NativeSubscriptionStorage.cpp',
    'src-gen/djinni/jni/NativeWakeLockImpl.cpp',
  ],
  header_namespace = '',
  headers = subdir_glob([
    ('', 'src-gen/**/*.hpp'),
  ]),
  compiler_flags = [
    '-std=c++11',
    '-fno-omit-frame-pointer',
    '-fexceptions',
    '-Wall',
    '-Werror',
  ],
  deps = [
    ':rocketspeed',
  ],
  visibility = [
    '//native/third-party/rocketspeed/...',
  ],
)

# Rocketbench tool (binary) -- debug build.
cxx_binary(
  name = 'rocketbench',
  srcs = [
    'src/port/port_posix.cc',
    'src/tools/rocketbench/random_distribution.cc',
    'src/tools/rocketbench/main.cc',
    'src/util/auto_roll_logger.cc',
    'src/util/env.cc',
    'src/util/env_posix.cc',
    'src/util/parsing.cc',
    'external/gflags/src/gflags.cc',
    'external/gflags/src/gflags_completions.cc',
    'external/gflags/src/gflags_reporting.cc',
  ],
  header_namespace = '',
  headers = subdir_glob([
    ('external/gflags/include', '*.h'),
    ('external/gflags/include', 'gflags/*.h'),
    ('external/gflags/include/gflags', '*.h'),
  ]),
  preprocessor_flags = [
    '-DGFLAGS=gflags',
  ],
  compiler_flags = [
    '-std=c++11',
    '-fno-omit-frame-pointer',
    '-fexceptions',
    '-Wall',
    '-Werror',
  ],
  deps = [
    ':rocketspeed_debug',
  ],
  visibility = [
    'PUBLIC',
  ],
)
