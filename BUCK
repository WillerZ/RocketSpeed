ndk_library(
  name = 'rocketspeed',
  deps = [
    '//native/third-party/libevent-2.0.21:libevent-2.0.21',
  ],
  flags = [
    'NDK_APPLICATION_MK=${PWD}/native/Application.mk',
  ],
  visibility = [
    '//native/rocketspeed:rocketspeed',
  ]
)

project_config(
  src_target = ':rocketspeed',
)
