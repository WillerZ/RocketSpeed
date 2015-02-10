// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "jvm_env.h"

#include <pthread.h>
#include <cassert>

namespace rocketspeed {

namespace {

JavaVM* java_vm_;
/** We use this key to determine which threads should be detached explicitly. */
pthread_key_t detach_thread_key;

jint AttachCurrentThread() {
  JNIEnv* env;
#if defined(OS_ANDROID)
  jint err = java_vm_->AttachCurrentThreadAsDaemon(&env, nullptr);
#else
  jint err = java_vm_->AttachCurrentThreadAsDaemon((void**)&env, nullptr);
#endif
  if (err != JNI_OK) {
    return err;
  } else if (!env) {
    return JNI_ERR;
  }
  if (pthread_setspecific(detach_thread_key, java_vm_)) {
    return JNI_ERR;
  }
  return JNI_OK;
}

void DetachCurrentThread(void* arg) {
  // arg is guaranteed to be non-null
  assert(arg);
  JavaVM* java_vm = reinterpret_cast<JavaVM*>(arg);
  if (JNI_OK != java_vm->DetachCurrentThread()) {
    // There is hardly anything we can do about error code from above call, so
    // we just ignore it.
    return;
  }
}

}  // namespace

jint JvmEnv::Init(JavaVM* java_vm) {
  java_vm_ = java_vm;
  if (pthread_key_create(&detach_thread_key, DetachCurrentThread)) {
    return JNI_ERR;
  }
  djinni::jniInit(java_vm_);
  return JNI_VERSION_1_6;
}

void JvmEnv::DeInit() {
  djinni::jniShutdown();
  java_vm_ = nullptr;
}

JvmEnv* JvmEnv::Default() {
  /** A default instance of JvmEnv. */
  static JvmEnv jvm_env_default_;
  return &jvm_env_default_;
}

BaseEnv::ThreadId JvmEnv::StartThread(void (*function)(void* arg),
                                      void* arg,
                                      const std::string& thread_name) {
  auto call = [function, arg]() {
    jint err = AttachCurrentThread();
    assert(err == JNI_OK);
    (void)err;
    (*function)(arg);
  };
  return ClientEnv::StartThread(std::move(call), thread_name);
}

BaseEnv::ThreadId JvmEnv::StartThread(std::function<void()> function,
                                      const std::string& thread_name) {
  auto call = [function]() {
    jint err = AttachCurrentThread();
    assert(err == JNI_OK);
    (void)err;
    function();
  };
  return ClientEnv::StartThread(std::move(call), thread_name);
}

}  // namespace rocketspeed
