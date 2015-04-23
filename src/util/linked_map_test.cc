#include "src/util/common/linked_map.h"
#include "src/util/testharness.h"
#include <memory>
#include <string>
#include <tuple>
#include <utility>

namespace rocketspeed {

class LinkedMapTest {};

TEST(LinkedMapTest, Constness) {  // Compile time test
  const LinkedMap<std::string, std::string> linked_map;
  linked_map.begin();
  linked_map.cbegin();
  linked_map.crbegin();
  linked_map.rbegin();
  linked_map.end();
  linked_map.cend();
  linked_map.rend();
  linked_map.crend();
  linked_map.empty();
  linked_map.size();
  linked_map.front();
  linked_map.back();
  linked_map.find("");
  linked_map.contains("");
}


struct NonCopyable {
  NonCopyable() {}
  NonCopyable(const NonCopyable&) = delete;
  NonCopyable& operator= (const NonCopyable&) = delete;
  NonCopyable(NonCopyable&&) = delete;
  NonCopyable& operator= (NonCopyable&&) = delete;
};

struct Movable {
  Movable() {}
  Movable(const Movable&) = delete;
  Movable& operator= (const Movable&) = delete;
  Movable(Movable&&) = default;
  Movable& operator= (Movable&&) = default;
};

struct Copyable {
  Copyable() {}
  Copyable(const Copyable&) = default;
  Copyable& operator= (const Copyable&) = default;
  Copyable(Copyable&&) = delete;
  Copyable& operator= (Copyable&&) = delete;
};

struct HashStub {
  template <class T> bool operator() (const T&) const { return 0; }
};

struct EqualStub {
  template <class T> bool operator() (const T& lhs, const T& rhs) const {
    return true;
  }
};

TEST(LinkedMapTest, InsertNonCopyable) {  // Compile time test
  using Type = NonCopyable;
  LinkedMap<Type, Type, HashStub, EqualStub> linked_map;
  linked_map.emplace(linked_map.begin(),
      std::piecewise_construct, std::tuple<>(), std::tuple<>());
  linked_map.emplace_front(
      std::piecewise_construct, std::tuple<>(), std::tuple<>());
  linked_map.emplace_back(
      std::piecewise_construct, std::tuple<>(), std::tuple<>());
}

TEST(LinkedMapTest, InsertMovable) {  // Compile time test
  using Type = Movable;
  LinkedMap<Type, Type, HashStub, EqualStub> linked_map;
  linked_map.insert(linked_map.begin(), std::make_pair(Type(), Type()));
  linked_map.emplace(linked_map.begin(), std::make_pair(Type(), Type()));
  linked_map.emplace(linked_map.begin(), Type(), Type());

  linked_map.push_front(std::make_pair(Type(), Type()));
  linked_map.emplace_front(std::make_pair(Type(), Type()));
  linked_map.emplace_front(Type(), Type());

  linked_map.push_back(std::make_pair(Type(), Type()));
  linked_map.emplace_back(std::make_pair(Type(), Type()));
  linked_map.emplace_back(Type(), Type());
}

TEST(LinkedMapTest, InsertCopyable) {  // Compile time test
  using Type = Copyable;
  LinkedMap<Type, Type, HashStub, EqualStub> linked_map;
  linked_map.insert(linked_map.begin(), std::make_pair(Type(), Type()));
  linked_map.emplace(linked_map.begin(), std::make_pair(Type(), Type()));
  linked_map.emplace(linked_map.begin(), Type(), Type());

  linked_map.push_front(std::make_pair(Type(), Type()));
  linked_map.emplace_front(std::make_pair(Type(), Type()));
  linked_map.emplace_front(Type(), Type());

  linked_map.push_back(std::make_pair(Type(), Type()));
  linked_map.emplace_back(std::make_pair(Type(), Type()));
  linked_map.emplace_back(Type(), Type());
}


TEST(LinkedMapTest, Emplace) {
  LinkedMap<int, int> linked_map;
  using Value = std::pair<const int, int>;
  const Value value1(1, 2);
  const Value value2(2, 3);
  const Value value3(3, 5);
  const Value valueX(1, 7);

  const auto result1 = linked_map.emplace_front(value1);
  ASSERT_TRUE (*result1.first == value1);
  ASSERT_TRUE (result1.second);
  ASSERT_TRUE (result1.first == linked_map.begin());
  ASSERT_TRUE (std::next(result1.first) == linked_map.end());

  const auto result2 = linked_map.emplace_back(value2);
  ASSERT_TRUE (*result2.first == value2);
  ASSERT_TRUE (result2.second);
  ASSERT_TRUE (std::prev(result2.first) == result1.first);
  ASSERT_TRUE (std::next(result2.first) == linked_map.end());

  const auto result3 = linked_map.emplace(result2.first, value3);
  ASSERT_TRUE (*result3.first == value3);
  ASSERT_TRUE (result3.second);
  ASSERT_TRUE (std::prev(result3.first) == result1.first);
  ASSERT_TRUE (std::next(result3.first) == result2.first);

  const auto resultX = linked_map.emplace(result3.first, valueX);
  ASSERT_TRUE (*resultX.first == value1);
  ASSERT_TRUE (!resultX.second);
  ASSERT_TRUE (resultX.first == result1.first);
}


TEST(LinkedMapTest, Find) {
  LinkedMap<int, int> linked_map;
  const auto& const_linked_map = linked_map;
  const auto it1 = linked_map.emplace_back(1, 2).first;
  const auto it2 = linked_map.emplace_back(2, 3).first;
  const auto it3 = linked_map.emplace_back(3, 5).first;

  ASSERT_TRUE (linked_map.find(1) == it1);
  ASSERT_TRUE (const_linked_map.find(1) == it1);

  ASSERT_TRUE (linked_map.find(2) == it2);
  ASSERT_TRUE (const_linked_map.find(2) == it2);

  ASSERT_TRUE (linked_map.find(3) == it3);
  ASSERT_TRUE (const_linked_map.find(3) == it3);

  ASSERT_TRUE (linked_map.find(-1) == linked_map.end());
  ASSERT_TRUE (const_linked_map.find(-1) == linked_map.end());
}


TEST(LinkedMapTest, Contains) {
  LinkedMap<int, int> linked_map;
  linked_map.emplace_back(1, 2);
  linked_map.emplace_back(2, 3);
  linked_map.emplace_back(3, 5);

  ASSERT_TRUE (linked_map.contains(1));
  ASSERT_TRUE (linked_map.contains(2));
  ASSERT_TRUE (linked_map.contains(3));
  ASSERT_TRUE (!linked_map.contains(-1));
}


TEST(LinkedMapTest, Erase) {
  LinkedMap<int, int> linked_map;
  const auto it1 = linked_map.emplace_back(1, 2).first;
  const auto it2 = linked_map.emplace_back(2, 3).first;
  const auto it3 = linked_map.emplace_back(3, 5).first;
  const auto it4 = linked_map.emplace_back(4, 7).first;

  ASSERT_TRUE (!linked_map.erase(-1));
  ASSERT_TRUE (linked_map.find(1) == it1);
  ASSERT_TRUE (linked_map.find(2) == it2);
  ASSERT_TRUE (linked_map.find(3) == it3);
  ASSERT_TRUE (linked_map.find(4) == it4);

  ASSERT_TRUE (linked_map.erase(2));
  ASSERT_TRUE (linked_map.find(1) == it1);
  ASSERT_TRUE (linked_map.find(2) == linked_map.end());
  ASSERT_TRUE (linked_map.find(3) == it3);
  ASSERT_TRUE (linked_map.find(4) == it4);

  ASSERT_TRUE (linked_map.erase(it3) == it4);
  ASSERT_TRUE (linked_map.find(1) == it1);
  ASSERT_TRUE (linked_map.find(3) == linked_map.end());
  ASSERT_TRUE (linked_map.find(4) == it4);

  linked_map.pop_front();
  ASSERT_TRUE (linked_map.find(1) == linked_map.end());
  ASSERT_TRUE (linked_map.find(4) == it4);

  linked_map.pop_back();
  ASSERT_TRUE (linked_map.find(4) == linked_map.end());
}


TEST(LinkedMapTest, Empty) {
  LinkedMap<int, int> linked_map;
  ASSERT_TRUE (linked_map.empty());

  linked_map.emplace_back(1, 2);
  ASSERT_TRUE (!linked_map.empty());

  linked_map.emplace_back(2, 3);
  ASSERT_TRUE (!linked_map.empty());

  linked_map.pop_front();
  ASSERT_TRUE (!linked_map.empty());

  linked_map.pop_front();
  ASSERT_TRUE (linked_map.empty());
}


TEST(LinkedMapTest, Size) {
  LinkedMap<int, int> linked_map;
  ASSERT_EQ (linked_map.size(), 0);

  linked_map.emplace_back(1, 2);
  ASSERT_EQ (linked_map.size(), 1);

  linked_map.emplace_back(2, 3);
  ASSERT_EQ (linked_map.size(), 2);

  linked_map.pop_front();
  ASSERT_EQ (linked_map.size(), 1);

  linked_map.pop_front();
  ASSERT_EQ (linked_map.size(), 0);
}


TEST(LinkedMapTest, Front) {
  LinkedMap<int, int> linked_map;
  const auto& const_linked_map = linked_map;

  using Value = std::pair<const int, int>;
  const Value value1(1, 2);
  const Value value2(2, 3);

  linked_map.push_back(value1);
  ASSERT_TRUE (linked_map.front() == value1);
  ASSERT_TRUE (const_linked_map.front() == value1);

  linked_map.push_back(value2);
  ASSERT_TRUE (linked_map.front() == value1);
  ASSERT_TRUE (const_linked_map.front() == value1);

  linked_map.pop_front();
  ASSERT_TRUE (linked_map.front() == value2);
  ASSERT_TRUE (const_linked_map.front() == value2);
}


TEST(LinkedMapTest, Back) {
  LinkedMap<int, int> linked_map;
  const auto& const_linked_map = linked_map;

  using Value = std::pair<const int, int>;
  const Value value1(1, 2);
  const Value value2(2, 3);

  linked_map.push_front(value1);
  ASSERT_TRUE (linked_map.back() == value1);
  ASSERT_TRUE (const_linked_map.back() == value1);

  linked_map.push_front(value2);
  ASSERT_TRUE (linked_map.back() == value1);
  ASSERT_TRUE (const_linked_map.back() == value1);

  linked_map.pop_back();
  ASSERT_TRUE (linked_map.back() == value2);
  ASSERT_TRUE (const_linked_map.back() == value2);
}


TEST(LinkedMapTest, MoveTo) {
  LinkedMap<int, int> linked_map;
  using Value = std::pair<const int, int>;

  using Values = std::vector<Value>;
  const auto DumpValues = [&]() -> Values {
    return Values(linked_map.begin(), linked_map.end());
  };

  const Value value1(1, 2);
  const Value value2(2, 3);
  const Value value3(3, 5);

  const auto it1 = linked_map.push_back(value1).first;
  const auto it2 = linked_map.push_back(value2).first;
  const auto it3 = linked_map.push_back(value3).first;

  linked_map.move_to(it1, it1);
  ASSERT_TRUE ((DumpValues() == Values{value1, value2, value3}));
  linked_map.move_to(it1, it2);
  ASSERT_TRUE ((DumpValues() == Values{value1, value2, value3}));
  linked_map.move_to(it1, it3);
  ASSERT_TRUE ((DumpValues() == Values{value2, value1, value3}));
  linked_map.move_to(it2, linked_map.end());
  ASSERT_TRUE ((DumpValues() == Values{value1, value3, value2}));

  linked_map.move_to(it3, it1);
  ASSERT_TRUE ((DumpValues() == Values{value3, value1, value2}));
  linked_map.move_to(it1, it1);
  ASSERT_TRUE ((DumpValues() == Values{value3, value1, value2}));
  linked_map.move_to(it1, it2);
  ASSERT_TRUE ((DumpValues() == Values{value3, value1, value2}));
  linked_map.move_to(it1, linked_map.end());
  ASSERT_TRUE ((DumpValues() == Values{value3, value2, value1}));

  linked_map.move_to(it1, it3);
  ASSERT_TRUE ((DumpValues() == Values{value1, value3, value2}));
  linked_map.move_to(it2, it3);
  ASSERT_TRUE ((DumpValues() == Values{value1, value2, value3}));
  linked_map.move_to(it3, it3);
  ASSERT_TRUE ((DumpValues() == Values{value1, value2, value3}));
  linked_map.move_to(it3, linked_map.end());
  ASSERT_TRUE ((DumpValues() == Values{value1, value2, value3}));
}

} // namespace rocketspeed


int main(int argc, char** argv) { return rocketspeed::test::RunAllTests(); }
