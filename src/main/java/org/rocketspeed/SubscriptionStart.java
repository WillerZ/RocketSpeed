package org.rocketspeed;

public class SubscriptionStart {

  private boolean present;
  private long sequenceNumber;

  public SubscriptionStart(long sequenceNumber) {
    present = true;
    this.sequenceNumber = sequenceNumber;
  }

  public SubscriptionStart() {
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
