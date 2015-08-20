package org.rocketspeed;

public abstract class RocketeerWithParams {
  public abstract void handleNewSubscription(InboundID inboundId, SubscriptionParameters params);

  public abstract void handleTermination(InboundID inboundId, SubscriptionParameters params);
}
