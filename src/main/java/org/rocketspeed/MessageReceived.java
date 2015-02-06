package org.rocketspeed;

public class MessageReceived {

  private final ClientImpl clientImpl;
  private final int namespaceId;
  private final String topicName;
  private final long sequenceNumber;
  private final byte[] contents;

  /* package */ MessageReceived(ClientImpl clientImpl, int namespaceId, String topicName,
                                long sequenceNumber, byte[] contents) {
    this.clientImpl = clientImpl;
    this.namespaceId = namespaceId;
    this.topicName = topicName;
    this.sequenceNumber = sequenceNumber;
    this.contents = contents;
  }

  public int getNamespaceId() {
    return namespaceId;
  }

  public String getTopicName() {
    return topicName;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  public byte[] getContents() {
    return contents;
  }

  public void acknowledge() {
    clientImpl.Acknowledge(namespaceId, topicName, sequenceNumber);
  }
}
