package org.rocketspeed.android;

/**
 * If {@link org.rocketspeed.Client} is to be run on Android device, an implementation of this
 * interface should be provided to the {@link org.rocketspeed.Builder} in order for the client to
 * acquire it when processing a message and release it when it's waiting for a message to arrive.
 */
public interface WakeLock {

  /**
   * Acquires the wake lock.
   */
  public void acquire();

  /**
   * Acquires the wake lock with a timeout.
   *
   * @param timeout The timeout (in milliseconds) after which to release the wake lock.
   */
  public void acquire(long timeout);

  /**
   * Releases the wake lock.
   */
  public void release();
}
