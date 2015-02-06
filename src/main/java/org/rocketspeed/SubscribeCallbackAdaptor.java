package org.rocketspeed;

import java.util.logging.Level;

/* package */ class SubscribeCallbackAdaptor extends SubscribeCallbackImpl {

  private final SubscribeCallback callback;

  /* package */ SubscribeCallbackAdaptor(SubscribeCallback callback) {
    this.callback = callback;
  }

  @Override
  public void Call(final Status status, final int namespaceId, final String topicName,
                   final long sequenceNumber, final boolean subscribed) {
    try {
      callback.call(status, namespaceId, topicName, sequenceNumber, subscribed);
    } catch (Throwable e) {
      Client.LOGGER.log(Level.WARNING, "Exception thrown in subscribe callback", e);
    }
  }
}
