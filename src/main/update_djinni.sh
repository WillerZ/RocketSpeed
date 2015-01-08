#!/usr/bin/env bash

REPO_ROOT="$(dirname "$0")/../.."
DJINNI_HOME="$REPO_ROOT/external/djinni"
IFACE_FILE="$REPO_ROOT/src/main/rocketspeed.djinni"
JAVA_PACKAGE="org.rocketspeed"
JAVA_DIR="$REPO_ROOT/target/generated-sources/djinni/java/${JAVA_PACKAGE/.//}"
CPP_DIR="$REPO_ROOT/target/generated-sources/djinni/cpp"

set -e
set +x

rm -rf \
  "$JAVA_DIR" \
  "$CPP_DIR"

"$DJINNI_HOME/src/run" \
  --idl "$IFACE_FILE" \
  \
  --java-out "$JAVA_DIR" \
  --java-package "$JAVA_PACKAGE" \
  --ident-java-field mFooBar \
  \
  --cpp-out "$CPP_DIR" \
  --cpp-namespace rocketspeed::djinni \
  --jni-out "$CPP_DIR" \
  --ident-jni-class NativeFooBar \
  --ident-jni-file NativeFooBar

# EOF
