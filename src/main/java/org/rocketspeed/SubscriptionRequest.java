package org.rocketspeed;

public class SubscriptionRequest {

  private final SubscriptionRequestImpl impl;

  public SubscriptionRequest(short namespaceId, String topicName, boolean subscribe,
                             SubscriptionStart start) {
    impl =
        new SubscriptionRequestImpl(namespaceId, topicName, subscribe, start.djinni());
  }

  /* package */ SubscriptionRequestImpl djinni() {
    return impl;
  }
}
