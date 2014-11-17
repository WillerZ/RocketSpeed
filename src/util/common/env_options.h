// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

namespace rocketspeed {

// Options while opening a file to read/write
class EnvOptions {
 public:

  // construct with default Options
  EnvOptions() {}

  // If true, then allow caching of data in environment buffers
  bool use_os_buffer = true;

   // If true, then use mmap to read data
  bool use_mmap_reads = false;

   // If true, then use mmap to write data
  bool use_mmap_writes = true;

  // If true, set the FD_CLOEXEC on open fd.
  bool set_fd_cloexec = true;

  // If true, we will preallocate the file with FALLOC_FL_KEEP_SIZE flag, which
  // means that file size won't change as part of preallocation.
  // If false, preallocation will also change the file size. This option will
  // improve the performance in workloads where you sync the data on every
  // write. By default, we set it to true for MANIFEST writes and false for
  // WAL writes
  bool fallocate_with_keep_size = true;

  // Send and receive buffer sizes for TCP connections.
  // Set to 0 to use OS defaults.
  int tcp_send_buffer_size = 1 << 20;
  int tcp_recv_buffer_size = 1 << 20;
};

}  // namespace rocketspeed
