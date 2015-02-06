package org.rocketspeed;

import java.util.logging.Level;

/* package */ class ReceiveCallbackAdaptor extends ReceiveCallbackImpl {

  private final ReceiveCallback callback;
  private ClientImpl clientImpl;

  /* package */ ReceiveCallbackAdaptor(ReceiveCallback callback) {
    this.callback = callback;
  }

  @Override
  public void Call(final int namespaceId, final String topicName, final long sequenceNumber,
                   final byte[] contents) {
    assert clientImpl != null;
    try {
      callback.call(
          new MessageReceived(clientImpl, namespaceId, topicName, sequenceNumber, contents));
    } catch (Throwable e) {
      Client.LOGGER.log(Level.WARNING, "Exception thrown in receive callback", e);
    }
  }

  public void setClientImpl(ClientImpl clientImpl) {
    assert this.clientImpl == null;
    this.clientImpl = clientImpl;
  }
}
