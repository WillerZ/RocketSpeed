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
  private ReceiveCallbackAdaptor receiveCallback;
  private SubscriptionStorage storage;
  private boolean restoreSubscriptions;
  private boolean resubscribeFromStorage;

  /**
   * A constructor to be used by non-Android applications.
   */
  public Builder() {
    reset();
  }

  /**
   * A constructor to be used by Android applications.
   *
   * @param wakeLock A wake lock implementation to be used by the client.
   */
  public Builder(WakeLock wakeLock) {
    this();
    assertNotNull(wakeLock, "Wake lock");
    this.wakeLock = wakeLock;
  }


  private void reset() {
    wakeLock = null;
    // We do not reset config, as it can be reused by multiple clients.
    tenantID = -1;
    clientID = null;
    subscribeCallback = null;
    // Receive callback adaptor is tied to each client, so it must be recreated before we can
    // reuse builder.
    receiveCallback = null;
    storage = new SubscriptionStorage(StorageType.NONE, "");
    restoreSubscriptions = false;
    resubscribeFromStorage = false;
  }

  public Builder configuration(Configuration config) {
    assertNotNull(config, "Configuration");
    this.config = config.djinni();
    return this;
  }

  public Builder tenantID(int tenantID) {
    this.tenantID = tenantID;
    return this;
  }

  public Builder clientID(String clientID) {
    assertNotNull(clientID, "ClientID");
    this.clientID = clientID;
    return this;
  }

  public Builder subscribeCallback(final SubscribeCallback callback) {
    assertNotNull(callback, "Subscribe callback");
    this.subscribeCallback = new SubscribeCallbackAdaptor(callback);
    return this;
  }

  public Builder receiveCallback(final ReceiveCallback callback) {
    assertNotNull(callback, "Receive callback");
    this.receiveCallback = new ReceiveCallbackAdaptor(callback);
    return this;
  }

  public Builder usingFileStorage(String filePath) {
    return usingFileStorage(filePath, true);
  }

  public Builder usingFileStorage(String filePath, boolean restoreSubscriptions) {
    assertNotNull(filePath, "Storage file path");
    storage = new SubscriptionStorage(StorageType.FILE, filePath);
    this.restoreSubscriptions = restoreSubscriptions;
    return this;
  }

  public Builder resubscribeFromStorage() {
    resubscribeFromStorage = true;
    return this;
  }

  public Client build() throws Exception {
    try {
      assertInvalidState(config != null, "Missing Configuration.");
      assertInvalidState(clientID != null, "Missing ClientID.");
      assertInvalidState(tenantID >= 0, "Missing TenantID.");
      ClientImpl clientImpl = ClientImpl.Open(config, tenantID, clientID, subscribeCallback,
                                              receiveCallback, storage, wrapWakeLock(wakeLock));
      try {
        // Note that until we call Start method on ClientImpl, no client threads are running.
        // Consequently, no callback can be issued by the client until it is started, therefore
        // it is safe to finish initialisation of receive callback now.
        receiveCallback.setClientImpl(clientImpl);
        clientImpl.Start(restoreSubscriptions, resubscribeFromStorage).checkExceptions();
        return new Client(clientImpl);
      } catch (Exception e) {
        clientImpl.Close();
        throw e;
      }
    } finally {
      reset();
    }
  }

  private void assertNotNull(Object param, String name) {
    if (param == null) {
      throw new NullPointerException(name + " must not be null.");
    }
  }

  private void assertInvalidState(boolean cond, String desc) {
    if (!cond) {
      throw new IllegalStateException(desc);
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
