// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>
#include <vector>

namespace rocketspeed {

/**
 * Splits a string with a provided delimiter. Does no trimming or other
 * massaging.
 *
 * @param s The string to split.
 * @param delim The character to split on.
 * @return A vector of the sub-strings.
 */
std::vector<std::string> SplitString(const std::string& s, char delim = ',');

}  // namespace rocketspeed
