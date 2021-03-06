// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

namespace rocketspeed {

std::vector<std::string> SplitString(const std::string& s, char delim = ',') {
  std::vector<std::string> r;
  r.reserve(std::count(s.begin(), s.end(), delim) + 1);
  auto first = s.begin();
  for (auto last = first; first != s.end(); first = last) {
    last = std::find(first, s.end(), delim);
    r.emplace_back(first, last);
    if (last != s.end()) {
      ++last;  // move past delim
    }
  }
  return r;
}

std::unordered_map<std::string, std::string> ParseMap(const std::string& s) {
  auto list = SplitString(s, ';');
  std::unordered_map<std::string, std::string> map;
  for (const auto& entry : list) {
    auto pair = SplitString(entry, '=');
    pair.resize(2);
    map.emplace(pair[0], pair[1]);
  }
  return map;
}

}  // namespace rocketspeed
