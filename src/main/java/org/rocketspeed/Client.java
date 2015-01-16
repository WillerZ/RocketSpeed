package org.rocketspeed;

import java.util.ArrayList;
import java.util.List;

import static org.rocketspeed.Types.fromUnsignedShort;
import static org.rocketspeed.Types.toUnsignedShort;

public class Client implements AutoCloseable {

  private final ClientImpl client;

  /* package */ Client(ClientImpl client) {
    this.client = client;
  }

  public MsgId publish(int namespaceID, String topicName, TopicOptions options, byte[] data)
      throws Exception {
    return publish(namespaceID, topicName, options, data, null, null);
  }

  public MsgId publish(int namespaceID, String topicName, TopicOptions options, byte[] data,
                       final PublishCallback callback) throws Exception {
    return publish(namespaceID, topicName, options, data, null, callback);
  }

  public MsgId publish(int namespaceID, String topicName, TopicOptions options, byte[] data,
                       MsgId messageId) throws Exception {
    return publish(namespaceID, topicName, options, data, messageId, null);
  }

  public MsgId publish(int namespaceID, String topicName, TopicOptions options, byte[] data,
                       MsgId messageId, final PublishCallback callback) throws Exception {
    MsgIdImpl messageId1 = messageId == null ? null : messageId.djinni();
    PublishCallbackImpl callback1 = callback == null ? null : new PublishCallbackImpl() {
      @Override
      public void Call(Status status, short namespaceId, String topicName, MsgIdImpl messageId,
                       long sequenceNumber, byte[] contents) {
        callback.call(status, toUnsignedShort(namespaceId), topicName, new MsgId(messageId),
                      sequenceNumber, contents);
      }
    };
    PublishStatus status =
        client.Publish(fromUnsignedShort(namespaceID), topicName, options.getRetention().djinni(),
                       data, messageId1, callback1);
    status.getStatus().checkExceptions();
    return new MsgId(status.getMessageId()

    );
  }

  public void listenTopics(List<SubscriptionRequest> requests) {
    ArrayList<SubscriptionRequestImpl> requests1 = new ArrayList<SubscriptionRequestImpl>();
    for (SubscriptionRequest request : requests) {
      requests1.add(request.djinni());
    }
    client.ListenTopics(requests1);
  }

  public void acknowledge(MessageReceived message) {
    client.Acknowledge(fromUnsignedShort(message.getNamespaceId()), message.getTopicName(),
                       message.getSequenceNumber());
  }

  @Override
  public void close() throws Exception {
    client.Close();
  }
}
