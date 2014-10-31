#include "MessageReceived_impl.h"

namespace rocketglue {

SequenceNumber MessageReceivedImpl::GetSequenceNumber() {
  return SequenceNumber(msg_.get()->GetSequenceNumber());
};

Topic MessageReceivedImpl::GetTopicName() {
  return Topic(msg_.get()->GetTopicName().ToString());
};


// XXX massive data copy, have to eliminate this.
std::vector<uint8_t> MessageReceivedImpl::GetContents() {
  rocketspeed::Slice sl = msg_.get()->GetContents();
  const char* p = sl.data();
  size_t size = sl.size();

  std::vector<uint8_t> tmp;
  for (unsigned int i = 0; i < size; i++) {
    tmp.push_back((uint8_t)*p++);
  }
  return tmp;
};

}
