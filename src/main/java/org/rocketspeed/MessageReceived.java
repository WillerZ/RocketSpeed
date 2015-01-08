package org.rocketspeed;

public class MessageReceived {

  private final short mNamespaceId;
  private final String mTopicName;
  private final long mSequenceNumber;
  private final byte[] mContents;

  public MessageReceived(short namespaceId, String topicName, long sequenceNumber,
                         byte[] contents) {
    this.mNamespaceId = namespaceId;
    this.mTopicName = topicName;
    this.mSequenceNumber = sequenceNumber;
    this.mContents = contents;
  }

  public short getNamespaceId() {
    return mNamespaceId;
  }

  public String getTopicName() {
    return mTopicName;
  }

  public long getSequenceNumber() {
    return mSequenceNumber;
  }

  public byte[] getContents() {
    return mContents;
  }
}
