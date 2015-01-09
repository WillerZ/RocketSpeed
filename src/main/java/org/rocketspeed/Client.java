package org.rocketspeed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Client implements AutoCloseable {

  private final ClientImpl client;

  /* package */ Client(ClientImpl client) {
    this.client = client;
  }

  MsgId publish(short namespaceID, String topicName, TopicOptions options, byte[] data)
      throws IOException {
    PublishStatus
        status =
        client.Publish0(namespaceID, topicName, options.getRetention().djinni(), data);
    status.getStatus().checkExceptions();
    return new MsgId(status.getMessageId());
  }

  MsgId publish(short namespaceID, String topicName, TopicOptions options, byte[] data,
                MsgId messageId) throws IOException {
    PublishStatus status =
        client.Publish1(namespaceID, topicName, options.getRetention().djinni(), data,
                        messageId.djinni());
    status.getStatus().checkExceptions();
    assert messageId.djinni().equals(status.getMessageId());
    return messageId;
  }

  void listenTopics(List<SubscriptionRequest> requests) {
    ArrayList<SubscriptionRequestImpl> requests1 = new ArrayList<SubscriptionRequestImpl>();
    for (SubscriptionRequest request : requests) {
      requests1.add(request.djinni());
    }
    client.ListenTopics(requests1);
  }

  void acknowledge(MessageReceived message) {
    client.Acknowledge(message.getNamespaceId(), message.getTopicName(),
                       message.getSequenceNumber(), message.getContents());
  }

  @Override
  public void close() throws Exception {
    client.Close();
  }
}
