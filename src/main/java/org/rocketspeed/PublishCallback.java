package org.rocketspeed;

public interface PublishCallback {

  void call(Status status, short namespaceId, String topicName, MsgId messageId,
            long sequenceNumber, byte[] contents);
}
