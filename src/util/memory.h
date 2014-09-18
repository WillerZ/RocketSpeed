// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

namespace rocketspeed {

/**
 * Equivalent to static_cast, but casts between std::unique_ptrs instead of
 * regular pointers. This only works with the default deleter.
 *
 * @param p pointer to cast from
 * @return the pointer downcast to type T. No type checking is done.
 */
template <typename T, typename U>
std::unique_ptr<T> unique_static_cast(std::unique_ptr<U>&& p) {
    return std::unique_ptr<T>(static_cast<T*>(p.release()));
}

}  // namespace rocketspeed
