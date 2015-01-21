package org.rocketspeed;

public interface PublishCallback {

  void call(Status status, int namespaceId, String topicName, MsgId messageId, long sequenceNumber);
}
