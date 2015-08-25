// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>
#include <unordered_map>
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

/**
 * Parses a string representation of a list of key-value pairs into an unordered
 * map. The string should consist of a ';'-delimited list of entries, while each
 * entry consists of a key and a value with '=' in between. Value for a key is
 * optional, if it is not provided the '=' terminating character is optional as
 * well.
 *
 * Valid strings and respective results:
 * 'key1=value1;key2=value2' => {'key1' : 'value1', 'key2' : 'value2'},
 * 'key1=;key2=value2' => {'key1' : '', 'key2' : 'value2'},
 * 'key1;key2==;=' => {'key1' : '', 'key2' : '=', '' : ''}.
 *
 * Invalid strings: none.
 *
 * @param s The string to parse.
 * @return A map
 */
std::unordered_map<std::string, std::string> ParseMap(const std::string& s);

}  // namespace rocketspeed
