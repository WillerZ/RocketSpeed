#!/usr/bin/env bash

REPO_ROOT="$(dirname "$0")/.."
DJINNI_HOME="$REPO_ROOT/external/djinni"
IFACE_FILE="$REPO_ROOT/src/main/rocketspeed.djinni"
JAVA_PACKAGE="org.rocketspeed"
JAVA_DIR="$REPO_ROOT/src/main/java-gen/${JAVA_PACKAGE/.//}"
CPP_DIR="$REPO_ROOT/src-gen/djinni"

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
  --ident-java-field fooBar \
  \
  --cpp-out "$CPP_DIR" \
  --cpp-namespace rocketspeed::djinni \
  \
  --jni-out "$CPP_DIR/jni" \
  --ident-jni-class NativeFooBar \
  --ident-jni-file NativeFooBar

# EOF
