// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#ifndef USE_UPSTREAM_LIBEVENT

#define event_new ld_event_new
#define event_add ld_event_add
#define event_free ld_event_free
#define event_del ld_event_del
#define event_get_fd ld_event_get_fd
#define event_base_loopexit ld_event_base_loopexit
#define event_base_new ld_event_base_new
#define event_base_dispatch ld_event_base_dispatch
#define event_base_free ld_event_base_free
#define evutil_make_socket_nonblocking ld_evutil_make_socket_nonblocking
#define evconnlistener_get_base ld_evconnlistener_get_base
#define evconnlistener_new_bind ld_evconnlistener_new_bind
#define evconnlistener_set_error_cb ld_evconnlistener_set_error_cb
#define evconnlistener_free ld_evconnlistener_free
#define event_enable_debug_logging ld_event_enable_debug_logging
#define event_set_log_callback ld_event_set_log_callback
#define event_enable_debug_mode ld_event_enable_debug_mode

#endif  // USE_UPSTREAM_LIBEVENT
