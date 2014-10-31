#! /usr/bin/env bash
#
# This files reads in rocketspeed.djinni and generates the
# stubs for java, c++, jni and objc.

# Locate the script file.  Cross symlinks if necessary.
loc="$0"
while [ -h "$loc" ]; do
    ls=`ls -ld "$loc"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        loc="$link"  # Absolute link
    else
        loc="`dirname "$loc"`/$link"  # Relative link
    fi
done
base_dir=$(cd `dirname "$loc"` && pwd)

# Directory where djinni is installed
djinni=$base_dir/../../external/djinni

# The generated code is placed here
out=$base_dir/generated

$djinni/src/run --idl rocketspeed.djinni \
                --java-out $out/java --java-package org.rocketspeed  --ident-java-field mFooBar \
                --cpp-out $out/c++ --cpp-namespace rocketglue \
                --jni-out $out/jni --ident-jni-class NativeFooBar --ident-jni-file NativeFooBar \
                --objc-out $out/objc --objcpp-namespace rocketglue --objc-type-prefix RS 
