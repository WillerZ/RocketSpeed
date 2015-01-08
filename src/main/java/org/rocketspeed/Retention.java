package org.rocketspeed;

public enum Retention {
  ONE_HOUR, ONE_DAY, ONE_WEEK;

  /* package */ RetentionBase djinni() {
    switch (this) {
      case ONE_HOUR:
        return RetentionBase.ONEHOUR;
      case ONE_DAY:
        return RetentionBase.ONEDAY;
      case ONE_WEEK:
        return RetentionBase.ONEWEEK;
      default:
        throw new IllegalStateException();
    }
  }
}
