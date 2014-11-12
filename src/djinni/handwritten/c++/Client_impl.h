#pragma GCC diagnostic ignored "-Wshadow" // Djinni isn't -Wshadow compatible

#include <climits>
#include <memory>
#include "./include/RocketSpeed.h"
#include "./src/djinni/generated/c++/Configuration.hpp"
#include "./src/djinni/generated/c++/Client.hpp"
#include "./src/djinni/generated/c++/Status.hpp"
#include "./src/djinni/generated/c++/SubscriptionStatus.hpp"
#include "./src/djinni/generated/c++/MessageReceived.hpp"
#include "./src/djinni/generated/c++/PublishCallback.hpp"
#include "./src/djinni/generated/c++/SubscribeCallback.hpp"
#include "./src/djinni/generated/c++/MessageReceivedCallback.hpp"

namespace rocketglue {

class ClientImpl : public Client {
 friend Client;
 public:
  void Publish(const Topic& topic_name,
               const NamespaceID& namespace_id,
               const TopicOptions& options,
               const std::vector<uint8_t>& data,
               const MsgId & msgid) override;

  void ListenTopics(const std::vector<SubscriptionPair>& names,
                    const TopicOptions& options) override;
 private:
  // The base rocketspeed client object
  std::unique_ptr<rocketspeed::Client> client_;
  std::unique_ptr<rocketspeed::Configuration> rs_config_;

  // Store the application-specified callbacks.
  // These callbacks are implemented in Java/objc.
  ClientID  client_id_;
  std::shared_ptr<Configuration> config_;
  std::shared_ptr<PublishCallback> publish_callback_;
  std::shared_ptr<SubscribeCallback> subscribe_callback_;
  std::shared_ptr<MessageReceivedCallback> receive_callback_;


  // private constructor
  ClientImpl(const ClientID& client_id,
             const std::shared_ptr<Configuration>& config,
             const std::shared_ptr<PublishCallback>& publish_callback,
             const std::shared_ptr<SubscribeCallback>& subscribe_callback,
             const std::shared_ptr<MessageReceivedCallback>& receive_callback);

  // Initialize the base RocketSpeed client
  Status Initialize();
};

}
