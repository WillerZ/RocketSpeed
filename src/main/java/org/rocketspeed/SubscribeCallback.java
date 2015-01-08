package org.rocketspeed;

public interface SubscribeCallback {

  void call(Status status, long sequenceNumber, boolean subscribed);
}
