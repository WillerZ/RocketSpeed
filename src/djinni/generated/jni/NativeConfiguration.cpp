// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#include "NativeConfiguration.hpp"  // my header
#include "HI32.hpp"
#include "HList.hpp"
#include "NativeConfiguration.hpp"
#include "NativeHostId.hpp"
#include "NativeTenantID.hpp"

namespace djinni_generated {

NativeConfiguration::NativeConfiguration() : djinni::JniInterfaceCppExt<::rocketglue::Configuration>("org/rocketspeed/Configuration$NativeProxy") {}

using namespace ::djinni_generated;

CJNIEXPORT void JNICALL Java_org_rocketspeed_Configuration_00024NativeProxy_nativeDestroy(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        delete reinterpret_cast<std::shared_ptr<::rocketglue::Configuration>*>(nativeRef);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, )
}

CJNIEXPORT jobject JNICALL Java_org_rocketspeed_Configuration_CreateNewInstance(JNIEnv* jniEnv, jobject /*this*/, jobject j_pilots, jobject j_copilots, jobject j_tenantId, jint j_port)
{
    try {
        DJINNI_FUNCTION_PROLOGUE0(jniEnv);
        std::vector<::rocketglue::HostId> c_pilots = ::djinni::HList<NativeHostId>::fromJava(jniEnv, j_pilots);
        jniEnv->DeleteLocalRef(j_pilots);
        std::vector<::rocketglue::HostId> c_copilots = ::djinni::HList<NativeHostId>::fromJava(jniEnv, j_copilots);
        jniEnv->DeleteLocalRef(j_copilots);
        ::rocketglue::TenantID c_tenant_id = NativeTenantID::fromJava(jniEnv, j_tenantId);
        jniEnv->DeleteLocalRef(j_tenantId);
        int32_t c_port = ::djinni::HI32::Unboxed::fromJava(jniEnv, j_port);

        std::shared_ptr<::rocketglue::Configuration> cr = ::rocketglue::Configuration::CreateNewInstance(c_pilots, c_copilots, c_tenant_id, c_port);

        return NativeConfiguration::toJava(jniEnv, cr);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, 0  /* value doesn't matter */ )
}

CJNIEXPORT jobject JNICALL Java_org_rocketspeed_Configuration_00024NativeProxy_native_1GetPilotHostIds(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketglue::Configuration> & ref = *reinterpret_cast<const std::shared_ptr<::rocketglue::Configuration>*>(nativeRef);

        std::vector<::rocketglue::HostId> cr = ref->GetPilotHostIds();

        return ::djinni::HList<NativeHostId>::toJava(jniEnv, cr);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, 0 /* value doesn't matter*/)
}

CJNIEXPORT jobject JNICALL Java_org_rocketspeed_Configuration_00024NativeProxy_native_1GetCopilotHostIds(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketglue::Configuration> & ref = *reinterpret_cast<const std::shared_ptr<::rocketglue::Configuration>*>(nativeRef);

        std::vector<::rocketglue::HostId> cr = ref->GetCopilotHostIds();

        return ::djinni::HList<NativeHostId>::toJava(jniEnv, cr);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, 0 /* value doesn't matter*/)
}

CJNIEXPORT jobject JNICALL Java_org_rocketspeed_Configuration_00024NativeProxy_native_1GetTenantID(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketglue::Configuration> & ref = *reinterpret_cast<const std::shared_ptr<::rocketglue::Configuration>*>(nativeRef);

        ::rocketglue::TenantID cr = ref->GetTenantID();

        return NativeTenantID::toJava(jniEnv, cr);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, 0 /* value doesn't matter*/)
}

CJNIEXPORT jint JNICALL Java_org_rocketspeed_Configuration_00024NativeProxy_native_1GetClientPort(JNIEnv* jniEnv, jobject /*this*/, jlong nativeRef)
{
    try {
        DJINNI_FUNCTION_PROLOGUE1(jniEnv, nativeRef);
        const std::shared_ptr<::rocketglue::Configuration> & ref = *reinterpret_cast<const std::shared_ptr<::rocketglue::Configuration>*>(nativeRef);

        int32_t cr = ref->GetClientPort();

        return ::djinni::HI32::Unboxed::toJava(jniEnv, cr);
    } JNI_TRANSLATE_EXCEPTIONS_RETURN(jniEnv, 0 /* value doesn't matter*/)
}

}  // namespace djinni_generated
