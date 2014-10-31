// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#include "NativeSubscriptionStatus.hpp"  // my header
#include "HBool.hpp"
#include "NativeSequenceNumber.hpp"
#include "NativeStatus.hpp"

namespace djinni_generated {

jobject NativeSubscriptionStatus::toJava(JNIEnv* jniEnv, ::rocketglue::SubscriptionStatus c) {
    djinni::LocalRef<jobject> j_status(jniEnv, NativeStatus::toJava(jniEnv, c.status));
    djinni::LocalRef<jobject> j_seqno(jniEnv, NativeSequenceNumber::toJava(jniEnv, c.seqno));
    jboolean j_subscribed = ::djinni::HBool::Unboxed::toJava(jniEnv, c.subscribed);
    const auto & data = djinni::JniClass<::djinni_generated::NativeSubscriptionStatus>::get();
    jobject r = jniEnv->NewObject(data.clazz.get(), data.jconstructor, j_status.get(), j_seqno.get(), j_subscribed);
    djinni::jniExceptionCheck(jniEnv);
    return r;
}

::rocketglue::SubscriptionStatus NativeSubscriptionStatus::fromJava(JNIEnv* jniEnv, jobject j) {
    assert(j != nullptr);
    const auto & data = djinni::JniClass<::djinni_generated::NativeSubscriptionStatus>::get();
    return ::rocketglue::SubscriptionStatus(
        NativeStatus::fromJava(jniEnv, djinni::LocalRef<jobject>(jniEnv, jniEnv->GetObjectField(j, data.field_mStatus)).get()),
        NativeSequenceNumber::fromJava(jniEnv, djinni::LocalRef<jobject>(jniEnv, jniEnv->GetObjectField(j, data.field_mSeqno)).get()),
        ::djinni::HBool::Unboxed::fromJava(jniEnv, jniEnv->GetBooleanField(j, data.field_mSubscribed)));
}

}  // namespace djinni_generated
