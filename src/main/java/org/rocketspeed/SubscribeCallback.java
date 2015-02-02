package org.rocketspeed;

public interface SubscribeCallback {

  void call(Status status, int namespaceId, String topicName, long sequenceNumber,
            boolean subscribed);
}
