package org.rocketspeed;

import java.util.logging.Level;

/* package */ class PublishCallbackAdaptor extends PublishCallbackImpl {

  private final PublishCallback callback;

  /* package */ PublishCallbackAdaptor(PublishCallback callback) {
    this.callback = callback;
  }

  @Override
  public void Call(Status status, String namespaceId, String topicName, MsgIdImpl messageId,
                   long sequenceNumber) {
    try {
      callback.call(status, namespaceId, topicName, new MsgId(messageId), sequenceNumber);
    } catch (Throwable e) {
      Client.LOGGER.log(Level.WARNING, "Exception thrown in publish callback", e);
    }
  }
}
