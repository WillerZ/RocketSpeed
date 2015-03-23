package org.rocketspeed;

public interface SubscribeCallback {

  void call(Status status, String namespaceId, String topicName, long sequenceNumber,
            boolean subscribed);
}
