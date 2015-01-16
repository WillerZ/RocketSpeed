package org.rocketspeed;

public class MessageReceived {

  private final int mNamespaceId;
  private final String mTopicName;
  private final long mSequenceNumber;
  private final byte[] mContents;

  public MessageReceived(int namespaceId, String topicName, long sequenceNumber, byte[] contents) {
    this.mNamespaceId = namespaceId;
    this.mTopicName = topicName;
    this.mSequenceNumber = sequenceNumber;
    this.mContents = contents;
  }

  public int getNamespaceId() {
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
