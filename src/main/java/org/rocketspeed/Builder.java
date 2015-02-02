package org.rocketspeed;

import org.rocketspeed.android.WakeLock;

import java.util.logging.Level;

public final class Builder {

  static {
    System.loadLibrary("rocketspeedjni");
  }

  private WakeLock wakeLock;
  private ConfigurationImpl config;
  private int tenantID;
  private String clientID;
  private SubscribeCallbackImpl subscribeCallback;
  private ReceiveCallbackImpl receiveCallback;
  private SubscriptionStorage storage;

  /**
   * A constructor to be used by non-Android applications.
   */
  public Builder() {
    reset();
  }

  /**
   * A constructor to be used by Android applications.
   */
  public Builder(WakeLock wakeLock) {
    assertNotNull(wakeLock, "Wake lock");
    reset();
    this.wakeLock = wakeLock;
  }


  private void reset() {
    wakeLock = null;
    // We do not reset config, as it can be reused by multiple clients.
    tenantID = -1;
    clientID = null;
    subscribeCallback = null;
    receiveCallback = null;
    storage = new SubscriptionStorage(StorageType.NONE, "");
  }

  public Builder configuration(Configuration config) {
    this.config = config.djinni();
    return this;
  }

  public Builder tenantID(int tenantID) {
    this.tenantID = tenantID;
    return this;
  }

  public Builder clientID(String clientID) {
    this.clientID = clientID;
    return this;
  }

  public Builder subscribeCallback(final SubscribeCallback callback) {
    this.subscribeCallback = new SubscribeCallbackImpl() {
      @Override
      public void Call(final Status status, final int namespaceId, final String topicName,
                       final long sequenceNumber, final boolean subscribed) {
        try {
          callback.call(status, namespaceId, topicName, sequenceNumber, subscribed);
        } catch (Throwable e) {
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
        } catch (Throwable e) {
          Client.LOGGER.log(Level.WARNING, "Exception thrown in receive callback", e);
        }
      }
    };
    return this;
  }

  public Builder usingFileStorage(String filePath) {
    storage = new SubscriptionStorage(StorageType.FILE, filePath);
    return this;
  }

  public Client build() throws RuntimeException {
    assertNotNull(config, "Configuration");
    assertNotNull(clientID, "ClientID");
    assertNotNull(storage, "Storage type");
    if (tenantID < 0) {
      throw new IllegalStateException("Tenant ID is missing.");
    }
    ClientImpl client = ClientImpl.Open(config, tenantID, clientID, subscribeCallback,
                                        receiveCallback, storage, wrapWakeLock(wakeLock));
    // No-throw guarantee below this line until the end of the function.
    reset();
    return new Client(client);
  }

  private void assertNotNull(Object param, String name) {
    if (param == null) {
      throw new IllegalStateException(name + " is missing.");
    }
  }

  private static WakeLockImpl wrapWakeLock(final WakeLock wakeLock) {
    return wakeLock == null ? null : new WakeLockImpl() {
      @Override
      public void Acquire(long timeout) {
        try {
          if (timeout < 0) {
            wakeLock.acquire();
          } else {
            wakeLock.acquire(timeout);
          }
        } catch (Throwable e) {
          Client.LOGGER.log(Level.WARNING, "Exception thrown in WakeLock.acquire()", e);
        }
      }

      @Override
      public void Release() {
        try {
          wakeLock.release();
        } catch (Throwable e) {
          Client.LOGGER.log(Level.WARNING, "Exception thrown in WakeLock.release()", e);
        }
      }
    };
  }
}
