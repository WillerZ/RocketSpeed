package org.rocketspeed;

public class SubscriptionStart {

  private SubscriptionStartImpl impl;

  public SubscriptionStart(long sequenceNumber) {
    impl = new SubscriptionStartImpl(sequenceNumber, false);
  }

  public SubscriptionStart() {
    impl = new SubscriptionStartImpl(0, false);
  }

  public boolean isPresent() {
    return impl.getPresent();
  }

  public long getSequenceNumber() {
    if (!isPresent()) {
      throw new IllegalStateException("Sequence number is absent.");
    }
    return impl.getSequenceNumber();
  }

  /* package */ SubscriptionStartImpl djinni() {
    return impl;
  }
}
