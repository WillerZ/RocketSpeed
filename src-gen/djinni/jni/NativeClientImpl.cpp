// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#include "NativeClientImpl.hpp"  // my header
#include "HBinary.hpp"
#include "HBool.hpp"
#include "HI32.hpp"
#include "HI64.hpp"
#include "HList.hpp"
#include "HOptional.hpp"
#include "HString.hpp"
#include "NativeClientImpl.hpp"
#include "NativeConfigurationImpl.hpp"
#include "NativeMsgIdImpl.hpp"
#include "NativePublishCallbackImpl.hpp"
#include "NativePublishStatus.hpp"
#include "NativeReceiveCallbackImpl.hpp"
#include "NativeRetentionBase.hpp"
#include "NativeSnapshotCallbackImpl.hpp"
#include "NativeStatus.hpp"
#include "NativeSubscribeCallbackImpl.hpp"
#include "NativeSubscriptionRequestImpl.hpp"
#include "NativeSubscriptionStorage.hpp"
#include "NativeWakeLockImpl.hpp"

namespace djinni_generated {

NativeClientImpl::NativeClientImpl() : djinni::JniInterface<::rocketspeed::djinni::ClientImpl, NativeClientImpl>("org/rocketspeed/ClientImpl$CppProxy") {}

using namespace ::djinni_generated;

CJNIEXPORT void JNICALL Java_org_rocketspeed_ClientImpl_00024CppProxy_nativeDestroy(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        delete reinterpret_cast<djinni::CppProxyHandle<::rocketspeed::djinni::ClientImpl>*>(nativeRef);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

CJNIEXPORT jobject JNICALL Java_org_rocketspeed_ClientImpl_Open(JNIEnv* jniEnv, jobject /*this*/, jobject j_config, jint j_tenantId, jstring j_clientId, jobject j_subscribeCallback, jobject j_receiveCallback, jobject j_storage, jobject j_wakeLock)
{
    try {
        DJINNI_FUNCTION_PROLOGUE0(jniEnv);
        ::rocketspeed::djinni::ConfigurationImpl c_config = NativeConfigurationImpl::fromJava(jniEnv, j_config);
        int32_t c_tenant_id = ::djinni::HI32::Unboxed::fromJava(jniEnv, j_tenantId);
        std::string c_client_id = ::djinni::HString::fromJava(jniEnv, j_clientId);
        std::shared_ptr<::rocketspeed::djinni::SubscribeCallbackImpl> c_subscribe_callback = NativeSubscribeCallbackImpl::fromJava(jniEnv, j_subscribeCallback);
        std::shared_ptr<::rocketspeed::djinni::ReceiveCallbackImpl> c_receive_callback = NativeReceiveCallbackImpl::fromJava(jniEnv, j_receiveCallback);
        ::rocketspeed::djinni::SubscriptionStorage c_storage = NativeSubscriptionStorage::fromJava(jniEnv, j_storage);
        std::shared_ptr<::rocketspeed::djinni::WakeLockImpl> c_wake_lock = NativeWakeLockImpl::fromJava(jniEnv, j_wakeLock);

        std::shared_ptr<::rocketspeed::djinni::ClientImpl> cr = ::rocketspeed::djinni::ClientImpl::Open(std::move(c_config), std::move(c_tenant_id), std::move(c_client_id), std::move(c_subscribe_callback), std::move(c_receive_callback), std::move(c_storage), std::move(c_wake_lock));

        return NativeClientImpl::toJava(jniEnv, cr);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, 0  /* value doesn't matter */ )
}

CJNIEXPORT jobject JNICALL Java_org_rocketspeed_ClientImpl_00024CppProxy_native_1Start(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef, jboolean j_restoreSubscriptions, jboolean j_resubscribeFromStorage)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketspeed::djinni::ClientImpl> & ref = djinni::CppProxyHandle<::rocketspeed::djinni::ClientImpl>::get(nativeRef);
        bool c_restore_subscriptions = ::djinni::HBool::Unboxed::fromJava(jniEnv, j_restoreSubscriptions);
        bool c_resubscribe_from_storage = ::djinni::HBool::Unboxed::fromJava(jniEnv, j_resubscribeFromStorage);

        ::rocketspeed::djinni::Status cr = ref->Start(std::move(c_restore_subscriptions), std::move(c_resubscribe_from_storage));

        return NativeStatus::toJava(jniEnv, cr);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, 0 /* value doesn't matter*/)
}

CJNIEXPORT jobject JNICALL Java_org_rocketspeed_ClientImpl_00024CppProxy_native_1Publish(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef, jint j_namespaceId, jstring j_topicName, jobject j_retention, jbyteArray j_data, jobject j_messageId, jobject j_publishCallback)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketspeed::djinni::ClientImpl> & ref = djinni::CppProxyHandle<::rocketspeed::djinni::ClientImpl>::get(nativeRef);
        int32_t c_namespace_id = ::djinni::HI32::Unboxed::fromJava(jniEnv, j_namespaceId);
        std::string c_topic_name = ::djinni::HString::fromJava(jniEnv, j_topicName);
        ::rocketspeed::djinni::RetentionBase c_retention = NativeRetentionBase::fromJava(jniEnv, j_retention);
        std::vector<uint8_t> c_data = ::djinni::HBinary::fromJava(jniEnv, j_data);
        std::experimental::optional<::rocketspeed::djinni::MsgIdImpl> c_message_id = ::djinni::HOptional<std::experimental::optional, NativeMsgIdImpl>::fromJava(jniEnv, j_messageId);
        std::shared_ptr<::rocketspeed::djinni::PublishCallbackImpl> c_publish_callback = NativePublishCallbackImpl::fromJava(jniEnv, j_publishCallback);

        ::rocketspeed::djinni::PublishStatus cr = ref->Publish(std::move(c_namespace_id), std::move(c_topic_name), std::move(c_retention), std::move(c_data), std::move(c_message_id), std::move(c_publish_callback));

        return NativePublishStatus::toJava(jniEnv, cr);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, 0 /* value doesn't matter*/)
}

CJNIEXPORT void JNICALL Java_org_rocketspeed_ClientImpl_00024CppProxy_native_1ListenTopics(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef, jobject j_names)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketspeed::djinni::ClientImpl> & ref = djinni::CppProxyHandle<::rocketspeed::djinni::ClientImpl>::get(nativeRef);
        std::vector<::rocketspeed::djinni::SubscriptionRequestImpl> c_names = ::djinni::HList<NativeSubscriptionRequestImpl>::fromJava(jniEnv, j_names);

        ref->ListenTopics(std::move(c_names));
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

CJNIEXPORT void JNICALL Java_org_rocketspeed_ClientImpl_00024CppProxy_native_1Acknowledge(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef, jint j_namespaceId, jstring j_topicName, jlong j_sequenceNumber)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketspeed::djinni::ClientImpl> & ref = djinni::CppProxyHandle<::rocketspeed::djinni::ClientImpl>::get(nativeRef);
        int32_t c_namespace_id = ::djinni::HI32::Unboxed::fromJava(jniEnv, j_namespaceId);
        std::string c_topic_name = ::djinni::HString::fromJava(jniEnv, j_topicName);
        int64_t c_sequence_number = ::djinni::HI64::Unboxed::fromJava(jniEnv, j_sequenceNumber);

        ref->Acknowledge(std::move(c_namespace_id), std::move(c_topic_name), std::move(c_sequence_number));
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

CJNIEXPORT void JNICALL Java_org_rocketspeed_ClientImpl_00024CppProxy_native_1SaveSubscriptions(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef, jobject j_snapshotCallback)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketspeed::djinni::ClientImpl> & ref = djinni::CppProxyHandle<::rocketspeed::djinni::ClientImpl>::get(nativeRef);
        std::shared_ptr<::rocketspeed::djinni::SnapshotCallbackImpl> c_snapshot_callback = NativeSnapshotCallbackImpl::fromJava(jniEnv, j_snapshotCallback);

        ref->SaveSubscriptions(std::move(c_snapshot_callback));
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

CJNIEXPORT void JNICALL Java_org_rocketspeed_ClientImpl_00024CppProxy_native_1Close(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketspeed::djinni::ClientImpl> & ref = djinni::CppProxyHandle<::rocketspeed::djinni::ClientImpl>::get(nativeRef);

        ref->Close();
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

}  // namespace djinni_generated
