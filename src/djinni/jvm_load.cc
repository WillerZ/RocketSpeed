//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "jni.h"

#include "src/djinni/jvm_env.h"

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* jvm, void* reserved) {
  return rocketspeed::JvmEnv::Init(jvm);
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* jvm, void* reserved) {
  rocketspeed::JvmEnv::DeInit();
}
