package org.rocketspeed;

import java.util.logging.Level;

/* package */ class SnapshotCallbackAdapter extends SnapshotCallbackImpl {

  private final SnapshotCallback callback;

  /* package */ SnapshotCallbackAdapter(SnapshotCallback callback) {
    this.callback = callback;
  }

  @Override
  public void Call(Status status) {
    try {
      callback.call(status);
    } catch (Throwable e) {
      Client.LOGGER.log(Level.WARNING, "Exception thrown in snapshot callback", e);
    }
  }
}
