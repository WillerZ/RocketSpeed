package org.rocketspeed;

import static org.rocketspeed.Types.fromUnsignedShort;

public class SubscriptionRequest {

  private final SubscriptionRequestImpl impl;

  public SubscriptionRequest(int namespaceId, String topicName, boolean subscribe,
                             SubscriptionStart start) {
    impl = new SubscriptionRequestImpl(fromUnsignedShort(namespaceId), topicName, subscribe,
                                       start.djinni());
  }

  /* package */ SubscriptionRequestImpl djinni() {
    return impl;
  }
}
