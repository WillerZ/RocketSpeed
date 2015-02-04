ndk_library(
  name = 'rocketspeed',
  deps = [
    '//native/third-party/libevent-2.0.21:libevent-2.0.21',
    '//native:base',
  ],
  flags = [
    'NDK_APPLICATION_MK=${PWD}/native/Application.mk',
    'ROCKETSPEED_RELEASE_BUILD=true', 
  ],
  visibility = [
    '//native/rocketspeed:rocketspeed',
  ]
)

#
# Build rocketspeed for debugging and tests
#
ndk_library(
  name = 'rocketspeed-test',
  deps = [
    '//native/third-party/libevent-2.0.21:libevent-test',
    '//native:base',
  ],
  flags = [
    'NDK_APPLICATION_MK=${PWD}/native/Application.mk',
    'ROCKETSPEED_RELEASE_BUILD=false', 
  ],
  visibility = [
    '//native/third-party/rocketspeed/...',
  ],
)

project_config(
  src_target = ':rocketspeed',
)

