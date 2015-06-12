package org.rocketspeed;

public final class Builder {

  static {
    System.loadLibrary("rsclientjni");
  }

  private LogLevel level;
  private HostId cockpit;
  private SubscriptionStorage storage;

  public Builder() {
    reset();
  }

  private void reset() {
    // Default log level includes warning messages.
    level = LogLevel.WARN_LEVEL;
    cockpit = null;
    storage = new SubscriptionStorage(StorageType.NONE, "");
  }

  public Builder logLevel(LogLevel level) {
    assertNotNull(level, "Log level");
    this.level = level;
    return this;
  }

  public Builder cockpit(HostId cockpit) {
    assertNotNull(cockpit, "Cockpit");
    this.cockpit = cockpit;
    return this;
  }

  public Builder usingFileStorage(String filePath) {
    assertNotNull(filePath, "Storage file path");
    storage = new SubscriptionStorage(StorageType.FILE, filePath);
    return this;
  }

  public Client build() throws Exception {
    try {
      assertInvalidState(cockpit != null, "Missing cockpit address.");
      return new Client(ClientImpl.create(level, cockpit, storage));
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
}
