#pragma GCC diagnostic ignored "-Wshadow" // Djinni isn't -Wshadow compatible

#include "./include/RocketSpeed.h"
#include "./src/djinni/generated/c++/MessageReceived.hpp"


namespace rocketglue {

class MessageReceivedImpl : public MessageReceived {
 public:
    MessageReceivedImpl(std::unique_ptr<rocketspeed::MessageReceived> msg) :
       msg_(std::move(msg)) {}

    ~MessageReceivedImpl()  {}
    SequenceNumber GetSequenceNumber() override;
    Topic GetTopicName() override;
    std::vector<uint8_t> GetContents() override;

 private:
  // The base rocketspeed config object
  std::unique_ptr<rocketspeed::MessageReceived> msg_;
};
}
