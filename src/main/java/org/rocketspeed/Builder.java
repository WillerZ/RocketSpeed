package org.rocketspeed;

import java.util.logging.Level;

public final class Builder {

  static {
    System.loadLibrary("rocketspeedjni");
  }

  private ConfigurationImpl config;
  private String clientID;
  private SubscribeCallbackImpl subscribeCallback;
  private ReceiveCallbackImpl receiveCallback;
  private SubscriptionStorage storage;

  public Builder() {
    reset();
  }

  private void reset() {
    config = null;
    clientID = null;
    subscribeCallback = null;
    receiveCallback = null;
    storage = new SubscriptionStorage(StorageType.NONE, "");
  }

  public Builder configuration(Configuration config) {
    this.config = config.djinni();
    return this;
  }

  public Builder clientID(String clientID) {
    this.clientID = clientID;
    return this;
  }

  public Builder subscribeCallback(final SubscribeCallback callback) {
    this.subscribeCallback = new SubscribeCallbackImpl() {
      @Override
      public void Call(final Status status, final long sequenceNumber, final boolean subscribed) {
        try {
          callback.call(status, sequenceNumber, subscribed);
        } catch (Exception e) {
          Client.LOGGER.log(Level.WARNING, "Exception thrown in subscribe callback", e);
        }
      }
    };
    return this;
  }

  public Builder receiveCallback(final ReceiveCallback callback) {
    this.receiveCallback = new ReceiveCallbackImpl() {
      @Override
      public void Call(final int namespaceId, final String topicName, final long sequenceNumber,
                       final byte[] contents) {
        try {
          callback.call(new MessageReceived(namespaceId, topicName, sequenceNumber, contents));
        } catch (Exception e) {
          Client.LOGGER.log(Level.WARNING, "Exception thrown in receive callback", e);
        }
      }
    };
    return this;
  }

  public Builder usingFileStorage(String filePath) {
    if (!StorageType.NONE.equals(storage.getType())) {
      throw new IllegalStateException();
    }
    storage = new SubscriptionStorage(StorageType.FILE, filePath);
    return this;
  }

  public Client build() {
    ClientImpl client =
        ClientImpl.Open(config, clientID, subscribeCallback, receiveCallback, storage);
    reset();
    return new Client(client);
  }
}
