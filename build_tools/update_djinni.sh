#!/usr/bin/env bash

REPO_ROOT="$(dirname "$0")/.."
DJINNI_HOME=${DJINNI_HOME:-"$REPO_ROOT/external/djinni"}
IFACE_FILE="$REPO_ROOT/src/djinni/rocketspeed.djinni"
JAVA_PACKAGE="org.rocketspeed"
JAVA_DIR="$REPO_ROOT/src-gen/djinni/java/${JAVA_PACKAGE/.//}"
CPP_DIR_REL="src-gen/djinni/cpp"
CPP_DIR="$REPO_ROOT/$CPP_DIR_REL"

set -e
set +x

find "$JAVA_DIR" "$CPP_DIR" -type f ! -name 'TARGETS' -delete || true

"$DJINNI_HOME/src/run" \
  --idl "$IFACE_FILE" \
  \
  --java-out "$JAVA_DIR" \
  --java-package "$JAVA_PACKAGE" \
  --ident-java-enum FOO_BAR \
  --ident-java-field fooBar \
  \
  --cpp-out "$CPP_DIR" \
  --cpp-include-prefix "$CPP_DIR_REL/" \
  --cpp-namespace rocketspeed::djinni \
  --cpp-optional-header "\"optional.hpp\"" \
  --cpp-optional-template "std::experimental::optional" \
  --ident-cpp-field foo_bar \
  --ident-cpp-method FooBar \
  --ident-cpp-type FooBar \
  --ident-cpp-enum-type FooBar \
  --ident-cpp-type-param FooBar \
  --ident-cpp-local foo_bar \
  --ident-cpp-file FooBar \
  \
  --jni-out "$CPP_DIR/jni" \
  --jni-include-prefix "$CPP_DIR_REL/jni/" \
  --jni-include-cpp-prefix "$CPP_DIR_REL/" \
  --ident-jni-class NativeFooBar \
  --ident-jni-file NativeFooBar \

# EOF
