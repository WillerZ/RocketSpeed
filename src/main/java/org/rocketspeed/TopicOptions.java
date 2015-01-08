package org.rocketspeed;

public class TopicOptions {

  private final Retention retention;

  public TopicOptions(Retention retention) {
    this.retention = retention;
  }

  public Retention getRetention() {
    return retention;
  }
}
