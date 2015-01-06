#!/usr/bin/env bash

REPO_ROOT="$(dirname "$0")/../.."
DJINNI_HOME="$REPO_ROOT/external/djinni"
IFACE_FILE="$REPO_ROOT/src/main/rocketspeed.djinni"
JAVA_DIR="$REPO_ROOT/src/main/java-gen"
CPP_DIR="$REPO_ROOT/src/main/cpp-gen"

set -e
set +x

rm -rf \
  "$JAVA_DIR" \
  "$CPP_DIR"

"$DJINNI_HOME/src/run" \
  --idl "$IFACE_FILE" \
  \
  --java-out "$JAVA_DIR" \
  --java-package org.rocketspeed  \
  --ident-java-field mFooBar \
  \
  --cpp-out "$CPP_DIR" \
  --cpp-namespace rocketspeed::djinni \
  --jni-out "$CPP_DIR" \
  --ident-jni-class NativeFooBar \
  --ident-jni-file NativeFooBar

rm -rf \
  "$DJINNI_PATH/src/target" \
  "$DJINNI_PATH/src/project/project" \
  "$DJINNI_PATH/src/project/target"

# EOF
