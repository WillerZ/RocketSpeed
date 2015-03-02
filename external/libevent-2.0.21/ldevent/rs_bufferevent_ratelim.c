#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshorten-64-to-32"
#pragma GCC diagnostic ignored "-Wunused-function"

#include "src/messages/event2_version.h"
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

#include <../bufferevent_ratelim.c>
#pragma GCC diagnostic pop
