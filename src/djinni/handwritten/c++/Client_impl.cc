#include "Client_impl.h"

namespace rocketglue {

//
// This is the structure of a received message. It provides
// the mapping from rocketspeed::Message to rocketglue::Message
// This class does a data-copy across language (java/c++/objc)
// boundaries but can be optimized in the future.
//
class Message : public MessageReceived {
 public:
  explicit Message(std::unique_ptr<rocketspeed::MessageReceived> msg) :
    msg_(std::move(msg)) {}

  SequenceNumber GetSequenceNumber() {
    return SequenceNumber(msg_->GetSequenceNumber());
  }

  Topic GetTopicName() {
    return Topic(msg_->GetTopicName().ToString());
  }

  std::vector<uint8_t> GetContents() {
    std::vector<uint8_t> contents;
    const char* data = msg_->GetContents().data();
    size_t size = msg_->GetContents().size();
    for (unsigned int i = 0; i < size; i++, data++) {
      uint8_t c = static_cast<uint8_t>(*data);
      contents.push_back(c);
    }
    return contents;
  }
 private:
  std::unique_ptr<rocketspeed::MessageReceived> msg_;
};

//
// This is a static method to create an instance of the RS client.
//
std::shared_ptr<Client>
Client::Open(const ClientID& client_id,
             const std::shared_ptr<Configuration>& config,
             const std::shared_ptr<PublishCallback>& publish_callback,
             const std::shared_ptr<SubscribeCallback>& subscribe_callback,
             const std::shared_ptr<MessageReceivedCallback>& receive_callback) {
  std::unique_ptr<ClientImpl> cl(new ClientImpl(
                                   client_id, config, publish_callback,
                                   subscribe_callback, receive_callback));
  if (cl) {
    Status status = cl->Initialize();
    if (status.code != StatusCode::KOK) {
      cl.reset(nullptr);
    }
  }
  return std::shared_ptr<Client>(cl.release());

};

//
// Constructor initializes the app specified callbacks.
//
ClientImpl::ClientImpl(const ClientID& client_id,
             const std::shared_ptr<Configuration>& config,
             const std::shared_ptr<PublishCallback>& publish_callback,
             const std::shared_ptr<SubscribeCallback>& subscribe_callback,
             const std::shared_ptr<MessageReceivedCallback>& receive_callback):
  client_(nullptr),
  rs_config_(nullptr),
  client_id_(client_id),
  config_(config),
  publish_callback_(publish_callback),
  subscribe_callback_(subscribe_callback),
  receive_callback_(receive_callback) {
};

//
// This is called at object construction time. It sets up the
// base RocketSpeed object.
//
Status
ClientImpl::Initialize() {
  // Create lambda that will be invoked when RS stores a message
  auto publish_callback = [&] (rocketspeed::ResultStatus rs) {
  };

  // Create a lambda that will be invoked by RS when a subscription
  // request is confirmed.
  auto subscribe_callback = [&] (rocketspeed::SubscriptionStatus ss) {
    Status status(StatusCode(StatusCode::KOK), "");
    if (!ss.status.ok()) {
      status.code = StatusCode::KINTERNAL;
      status.state = ss.status.ToString();
    }
    SubscriptionStatus s(status,
                         SequenceNumber(ss.seqno),
                         ss.subscribed);
    subscribe_callback_->Call(s);
  };

  // Create a lambda that will be invoked by RS when a message
  // is received.
  auto receive_callback = [&]
    (std::unique_ptr<rocketspeed::MessageReceived> rs) {
    std::shared_ptr<MessageReceived>
                  msg_received(new Message(std::move(rs)));
    receive_callback_->Call(msg_received);
  };

  // map TenantId
  if (config_->GetTenantID().tenantid > SHRT_MAX) {
    return Status(StatusCode::KINVALIDARGUMENT,
                  "TenantId has to fit in 2 bytes");
  }
  rocketspeed::TenantID tenant = config_->GetTenantID().tenantid;

  // map pilots and copilots from rocketglue to rocketspeed
  std::vector<rocketspeed::HostId> pilots;
  std::vector<rocketspeed::HostId> copilots;
  for (auto host : config_->GetPilotHostIds()) {
    pilots.push_back(rocketspeed::HostId(host.hostname, host.port));
  }
  for (auto host : config_->GetCopilotHostIds()) {
    copilots.push_back(rocketspeed::HostId(host.hostname, host.port));
  }

  // Create RS configuration
  rs_config_.reset(rocketspeed::Configuration::Create(
                     pilots, copilots, tenant, config_->GetClientPort()));

  // create RS client
  rocketspeed::Client* client;
  rocketspeed::Status status = rocketspeed::Client::Open(rs_config_.get(),
                                 client_id_.name,
                                 publish_callback,
                                 subscribe_callback,
                                 receive_callback,
                                 &client);
  if (!status.ok()) {
    return Status(StatusCode::KINTERNAL, status.ToString());
  }
  client_.reset(client);
  return Status(StatusCode::KOK, "");
};

void
ClientImpl::Publish(const Topic& topic_name,
                    const NamespaceID& namespace_id,
                    const TopicOptions& options,
                    const std::vector<uint8_t>& data,
                    const MsgId & msgid) {
};

void
ClientImpl::ListenTopics(const std::vector<SubscriptionPair>& names,
                         const TopicOptions& options) {
};

}
