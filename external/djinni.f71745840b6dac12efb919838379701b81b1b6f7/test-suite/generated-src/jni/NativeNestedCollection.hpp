// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from nested_collection.djinni

#pragma once

#include "djinni_support.hpp"
#include "nested_collection.hpp"

namespace djinni_generated {

class NativeNestedCollection final {
public:
    using CppType = NestedCollection;
    using JniType = jobject;

    static jobject toJava(JNIEnv*, NestedCollection);
    static NestedCollection fromJava(JNIEnv*, jobject);

    const djinni::GlobalRef<jclass> clazz { djinni::jniFindClass("com/dropbox/djinni/test/NestedCollection") };
    const jmethodID jconstructor { djinni::jniGetMethodID(clazz.get(), "<init>", "(Ljava/util/ArrayList;)V") };
    const jfieldID field_mSetList { djinni::jniGetFieldID(clazz.get(), "mSetList", "Ljava/util/ArrayList;") };

private:
    NativeNestedCollection() {}
    friend class djinni::JniClass<::djinni_generated::NativeNestedCollection>;
};

}  // namespace djinni_generated
