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
    return publish(namespaceID, topicName, options, data, null, null);
  }

  MsgId publish(short namespaceID, String topicName, TopicOptions options, byte[] data,
                final PublishCallback callback) throws IOException {
    return publish(namespaceID, topicName, options, data, null, callback);
  }

  MsgId publish(short namespaceID, String topicName, TopicOptions options, byte[] data,
                MsgId messageId) throws IOException {
    return publish(namespaceID, topicName, options, data, messageId, null);
  }

  MsgId publish(short namespaceID, String topicName, TopicOptions options, byte[] data,
                MsgId messageId, final PublishCallback callback) throws IOException {
    MsgIdImpl messageId1 = messageId == null ? null : messageId.djinni();
    PublishCallbackImpl callback1 = callback == null ? null : new PublishCallbackImpl() {
      @Override
      public void Call(Status status, short namespaceId, String topicName, MsgIdImpl messageId,
                       long sequenceNumber, byte[] contents) {
        callback.call(status, namespaceId, topicName, new MsgId(messageId), sequenceNumber,
                      contents);
      }
    };
    PublishStatus status = client.Publish(namespaceID, topicName, options.getRetention().djinni(),
                                          data, messageId1, callback1);
    status.getStatus().checkExceptions();
    return new MsgId(status.getMessageId()

    );
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
                       message.getSequenceNumber());
  }

  @Override
  public void close() throws Exception {
    client.Close();
  }
}
