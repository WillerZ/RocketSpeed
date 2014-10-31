// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#include "NativeClient.hpp"  // my header
#include "HBinary.hpp"
#include "HList.hpp"
#include "NativeConfiguration.hpp"
#include "NativeMessageReceivedCallback.hpp"
#include "NativeMsgId.hpp"
#include "NativeNamespaceID.hpp"
#include "NativePublishCallback.hpp"
#include "NativeSubscribeCallback.hpp"
#include "NativeSubscriptionPair.hpp"
#include "NativeTopic.hpp"
#include "NativeTopicOptions.hpp"

namespace djinni_generated {

NativeClient::NativeClient() : djinni::JniInterfaceCppExt<::rocketglue::Client>("org/rocketspeed/Client$NativeProxy") {}

using namespace ::djinni_generated;

CJNIEXPORT void JNICALL Java_org_rocketspeed_Client_00024NativeProxy_nativeDestroy(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        delete reinterpret_cast<std::shared_ptr<::rocketglue::Client>*>(nativeRef);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

CJNIEXPORT void JNICALL Java_org_rocketspeed_Client_00024NativeProxy_native_1Open(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef, jobject j_config, jobject j_publishCallback, jobject j_subscribeCallback, jobject j_receiveCallback)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketglue::Client> & ref = *reinterpret_cast<const std::shared_ptr<::rocketglue::Client>*>(nativeRef);
        std::shared_ptr<::rocketglue::Configuration> c_config = NativeConfiguration::fromJava(jniEnv, j_config);
        jniEnv->DeleteLocalRef(j_config);
        std::shared_ptr<::rocketglue::PublishCallback> c_publish_callback = NativePublishCallback::fromJava(jniEnv, j_publishCallback);
        jniEnv->DeleteLocalRef(j_publishCallback);
        std::shared_ptr<::rocketglue::SubscribeCallback> c_subscribe_callback = NativeSubscribeCallback::fromJava(jniEnv, j_subscribeCallback);
        jniEnv->DeleteLocalRef(j_subscribeCallback);
        std::shared_ptr<::rocketglue::MessageReceivedCallback> c_receive_callback = NativeMessageReceivedCallback::fromJava(jniEnv, j_receiveCallback);
        jniEnv->DeleteLocalRef(j_receiveCallback);

        ref->Open(c_config, c_publish_callback, c_subscribe_callback, c_receive_callback);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

CJNIEXPORT void JNICALL Java_org_rocketspeed_Client_00024NativeProxy_native_1Publish(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef, jobject j_topicName, jobject j_namespaceId, jobject j_options, jbyteArray j_data, jobject j_msgid)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketglue::Client> & ref = *reinterpret_cast<const std::shared_ptr<::rocketglue::Client>*>(nativeRef);
        ::rocketglue::Topic c_topic_name = NativeTopic::fromJava(jniEnv, j_topicName);
        jniEnv->DeleteLocalRef(j_topicName);
        ::rocketglue::NamespaceID c_namespace_id = NativeNamespaceID::fromJava(jniEnv, j_namespaceId);
        jniEnv->DeleteLocalRef(j_namespaceId);
        ::rocketglue::TopicOptions c_options = NativeTopicOptions::fromJava(jniEnv, j_options);
        jniEnv->DeleteLocalRef(j_options);
        std::vector<uint8_t> c_data = ::djinni::HBinary::fromJava(jniEnv, j_data);
        jniEnv->DeleteLocalRef(j_data);
        ::rocketglue::MsgId c_msgid = NativeMsgId::fromJava(jniEnv, j_msgid);
        jniEnv->DeleteLocalRef(j_msgid);

        ref->Publish(c_topic_name, c_namespace_id, c_options, c_data, c_msgid);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

CJNIEXPORT void JNICALL Java_org_rocketspeed_Client_00024NativeProxy_native_1ListenTopics(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef, jobject j_names, jobject j_options)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketglue::Client> & ref = *reinterpret_cast<const std::shared_ptr<::rocketglue::Client>*>(nativeRef);
        std::vector<::rocketglue::SubscriptionPair> c_names = ::djinni::HList<NativeSubscriptionPair>::fromJava(jniEnv, j_names);
        jniEnv->DeleteLocalRef(j_names);
        ::rocketglue::TopicOptions c_options = NativeTopicOptions::fromJava(jniEnv, j_options);
        jniEnv->DeleteLocalRef(j_options);

        ref->ListenTopics(c_names, c_options);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

}  // namespace djinni_generated
