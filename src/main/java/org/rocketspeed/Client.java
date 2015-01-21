package org.rocketspeed;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client implements AutoCloseable {

  /* package */ static final Logger LOGGER = Logger.getLogger(Client.class.getName());

  static {
    System.loadLibrary("rocketspeedjni");
  }

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
      public void Call(Status status, int namespaceId, String topicName, MsgIdImpl messageId,
                       long sequenceNumber) {
        try {
          callback.call(status, namespaceId, topicName, new MsgId(messageId), sequenceNumber);
        } catch (Exception e) {
          LOGGER.log(Level.WARNING, "Exception thrown in publish callback", e);
        }
      }
    };
    PublishStatus status =
        client.Publish(namespaceID, topicName, options.getRetention().djinni(), data, messageId1,
                       callback1);
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
    client.Acknowledge(message.getNamespaceId(), message.getTopicName(),
                       message.getSequenceNumber());
  }

  @Override
  public void close() throws Exception {
    client.Close();
  }
}
