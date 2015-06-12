package org.rocketspeed;

import java.util.ArrayList;

public class Client implements AutoCloseable {

  public static final long BEGINNING_SEQNO = Long.MIN_VALUE + 1L;
  public static final long CURRENT_SEQNO = Long.MIN_VALUE;
  public static final String GUEST_NAMESPACE = "guest";
  public static final int GUEST_TENANT = 1;

  static {
    System.loadLibrary("rsclientjni");
  }

  private final ClientImpl client;

  /* package */ Client(ClientImpl client) {
    this.client = client;
  }

  public MsgId publish(int tenantId,
                       String namespaceId,
                       String topicName,
                       byte[] data,
                       PublishCallback publishCb) {
    return publish(tenantId, namespaceId, topicName, data, publishCb, MsgId.EMPTY);
  }

  public MsgId publish(int tenantId,
                       String namespaceId,
                       String topicName,
                       byte[] data,
                       PublishCallback publishCb,
                       MsgId messageId) {
    return client.publish(tenantId, namespaceId, topicName, data, publishCb, messageId);
  }

  public long subscribe(int tenantId,
                        String namespaceId,
                        String topicName,
                        long startSeqno,
                        MessageReceivedCallback deliverCb) {
    return subscribe(tenantId, namespaceId, topicName, startSeqno, deliverCb, null);
  }

  public long subscribe(int tenantId,
                        String namespaceId,
                        String topicName,
                        long startSeqno,
                        MessageReceivedCallback deliverCb,
                        SubscribeCallback subscribeCb) {
    return client.subscribe(tenantId, namespaceId, topicName, startSeqno, deliverCb, subscribeCb);
  }

  public long subscribe(SubscriptionParameters params, MessageReceivedCallback deliverCb) {
    return subscribe(params, deliverCb, null);
  }

  public long subscribe(SubscriptionParameters params,
                        MessageReceivedCallback deliverCb,
                        SubscribeCallback subscribeCb) {
    return client.resubscribe(params, deliverCb, subscribeCb);
  }

  public void unsubscribe(long subHandle) {
    client.unsubscribe(subHandle);
  }

  public void acknowledge(long subHandle, long seqno) {
    client.acknowledge(subHandle, seqno);
  }

  public void saveSubscriptions(SnapshotCallback snapshotCb) {
    client.saveSubscriptions(snapshotCb);
  }

  public ArrayList<SubscriptionParameters> restoreSubscriptions() {
    return client.restoreSubscriptions();
  }

  @Override
  public void close() {
    client.close();
  }
}
