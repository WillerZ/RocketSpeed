// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/commands.h"

namespace rocketspeed {

MessageCommand::MessageCommand(const MsgId& msgid,
                               const HostId& recipient,
                               const Message& message)
: msgid_(msgid)
, recipient_(recipient)
, message_(message.Serialize().ToString()) {
}

}  // namespace rocketspeed
