package org.rocketspeed;

public class SubscriptionStart {

  public static final SubscriptionStart BEGINNING = new SubscriptionStart(Long.MIN_VALUE + 1L);
  public static final SubscriptionStart CURRENT = new SubscriptionStart(Long.MIN_VALUE);
  public static final SubscriptionStart UNKNOWN = new SubscriptionStart();
  private boolean present;
  private long sequenceNumber;

  public SubscriptionStart(long sequenceNumber) {
    present = true;
    this.sequenceNumber = sequenceNumber;
  }

  private SubscriptionStart() {
    present = false;
    sequenceNumber = 0;
  }

  public boolean isPresent() {
    return present;
  }

  public long getSequenceNumber() {
    if (!isPresent()) {
      throw new IllegalStateException("Sequence number is absent.");
    }
    return sequenceNumber;
  }

  /* package */ Long djinni() {
    return present ? sequenceNumber : null;
  }
}
