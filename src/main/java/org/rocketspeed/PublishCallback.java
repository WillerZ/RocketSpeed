package org.rocketspeed;

public interface PublishCallback {

  void call(Status status, String namespaceId, String topicName, MsgId messageId,
            long sequenceNumber);
}
