package org.rocketspeed;

import java.util.ArrayList;
import java.util.List;
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

  public MsgId publish(int tenantId, String namespaceID, String topicName, byte[] data) throws Exception {
    return publish(tenantId, namespaceID, topicName, data, null);
  }

  public MsgId publish(int tenantId, String namespaceID, String topicName, byte[] data, PublishCallback callback)
      throws Exception {
    return publish(tenantId, namespaceID, topicName, data, callback, new TopicOptions());
  }

  public MsgId publish(int tenantId, String namespaceID, String topicName, byte[] data, PublishCallback callback,
                       TopicOptions options) throws Exception {
    return publish(tenantId, namespaceID, topicName, data, callback, options, null);
  }

  public MsgId publish(int tenantId, String namespaceID, String topicName, byte[] data, PublishCallback callback,
                       TopicOptions options, MsgId messageId) throws Exception {
    assert options != null;
    MsgIdImpl messageId1 = messageId == null ? null : messageId.djinni();
    PublishCallbackImpl callback1 = callback == null ? null : new PublishCallbackAdaptor(callback);
    PublishStatus status = client.Publish(tenantId, namespaceID, topicName, data, messageId1, callback1);
    status.getStatus().checkExceptions();
    return new MsgId(status.getMessageId());
  }

  public void listenTopics(int tenantId, List<SubscriptionRequest> requests) {
    ArrayList<SubscriptionRequestImpl> requests1 = new ArrayList<SubscriptionRequestImpl>();
    for (SubscriptionRequest request : requests) {
      requests1.add(request.djinni());
    }
    client.ListenTopics(tenantId, requests1);
  }

  public void saveSubscriptions(SnapshotCallback callback) {
    client.SaveSubscriptions(new SnapshotCallbackAdapter(callback));
  }

  @Override
  public void close() {
    client.Close();
  }
}
