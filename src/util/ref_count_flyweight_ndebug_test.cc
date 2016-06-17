#ifndef NDEBUG
#define NDEBUG
#endif
#include "src/util/common/ref_count_flyweight.h"
#include "src/util/testharness.h"
#include <functional>

namespace rocketspeed {

class RefCountFlyweightTestNDebug : public ::testing::Test {};

TEST_F(RefCountFlyweightTestNDebug, NotLargerThanRawPointer) {
  ASSERT_LE(sizeof(RefCountFlyweight<std::string>), sizeof(void*));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
