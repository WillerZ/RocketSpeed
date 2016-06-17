#include "src/util/common/ref_count_flyweight.h"
#include "src/util/testharness.h"
#include <functional>

namespace rocketspeed {

class RefCountFlyweightTest : public ::testing::Test {};

TEST_F(RefCountFlyweightTest, AllTemplatesInstantiate) {  // Compile time test
  RefCountFlyweightFactory<std::string> factory;
  auto f1 = factory.GetFlyweight("");
  (void)f1.Get();
  auto f2(f1);             // copy constructor
  f2 = f1;                 // copy assignment
  auto f3(std::move(f1));  // move constructor
  f3 = std::move(f2);      // move assignment
  // + destructors
}

TEST_F(RefCountFlyweightTest, GetAfterAddReturnsAddedValue) {
  RefCountFlyweightFactory<int> factory;
  auto h1 = factory.GetFlyweight(1);
  auto h2 = factory.GetFlyweight(2);
  auto h3 = factory.GetFlyweight(3);
  ASSERT_EQ(h1.Get(), 1);
  ASSERT_EQ(h2.Get(), 2);
  ASSERT_EQ(h3.Get(), 3);
}

struct InstanceCounter {
  explicit InstanceCounter(int& instanceCount)
  : instanceCount_(&instanceCount) {
    ++(*instanceCount_);
  }
  InstanceCounter(const InstanceCounter& rhs)
  : instanceCount_(rhs.instanceCount_) {
    ++(*instanceCount_);
  }
  InstanceCounter& operator=(const InstanceCounter& rhs) {
    instanceCount_ = rhs.instanceCount_;
    ++(*instanceCount_);
    return *this;
  }
  ~InstanceCounter() { --(*instanceCount_); }
  bool operator<(const InstanceCounter& rhs) const {
    return instanceCount_ < rhs.instanceCount_;
  }
  int* instanceCount_;
  static const char* PAYLOAD;
  std::string payload_ = PAYLOAD;
};

const char* InstanceCounter::PAYLOAD =
    "12345678901234567890123456789012345678901234567890";  // should be long
                                                           // enough to be
                                                           // allocated on
                                                           // heap (not stored
                                                           // using
                                                           // small-string
                                                           // optimization)

TEST_F(RefCountFlyweightTest, DataNotDuppedMoreThanTwiceAndDestroyedAtEnd) {
  int instanceCount = 0;
  RefCountFlyweightFactory<InstanceCounter> factory;
  ASSERT_EQ(instanceCount, 0);

  {
    auto h1 = factory.GetFlyweight(InstanceCounter(instanceCount));
    ASSERT_LE(instanceCount, 2);
    auto h2 = factory.GetFlyweight(InstanceCounter(instanceCount));
    ASSERT_LE(instanceCount, 2);
    auto h3 = factory.GetFlyweight(InstanceCounter(instanceCount));
    ASSERT_LE(instanceCount, 2);
  }

  ASSERT_EQ(instanceCount, 0);
}

TEST_F(RefCountFlyweightTest, FlyweightsCanBeDestroyedBeforeOrAfterFactory) {
  int instanceCountUniqueAfter = 0;
  int instanceCountUniqueBefore = 0;
  int instanceCountShared = 0;

  {
    RefCountFlyweight<InstanceCounter> uniqueDestroyedAfterFactory;
    RefCountFlyweight<InstanceCounter> sharedDestroyedAfterFactory;
    {
      RefCountFlyweightFactory<InstanceCounter> factory;
      {
        RefCountFlyweight<InstanceCounter> uniqueDestroyedBeforeFactory;
        RefCountFlyweight<InstanceCounter> sharedDestroyedBeforeFactory;

        uniqueDestroyedBeforeFactory =
            factory.GetFlyweight(InstanceCounter(instanceCountUniqueBefore));
        sharedDestroyedBeforeFactory =
            factory.GetFlyweight(InstanceCounter(instanceCountShared));
        uniqueDestroyedAfterFactory =
            factory.GetFlyweight(InstanceCounter(instanceCountUniqueAfter));
        sharedDestroyedAfterFactory =
            factory.GetFlyweight(InstanceCounter(instanceCountShared));
      }
      ASSERT_EQ(instanceCountUniqueBefore, 0);
      // All good when flyweight is destroyed before factory
    }
    ASSERT_GT(instanceCountUniqueAfter, 0);
    ASSERT_GT(instanceCountShared, 0);
    ASSERT_EQ(uniqueDestroyedAfterFactory.Get().payload_,
              InstanceCounter::PAYLOAD);
    ASSERT_EQ(sharedDestroyedAfterFactory.Get().payload_,
              InstanceCounter::PAYLOAD);
    // All good when non-empty flyweight factory is destroyed
  }
  ASSERT_EQ(instanceCountUniqueAfter, 0);
  ASSERT_EQ(instanceCountShared, 0);
  // All good when flyweight is destroyed after factory
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
