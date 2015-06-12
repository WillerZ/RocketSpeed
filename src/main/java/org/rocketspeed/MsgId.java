package org.rocketspeed;

public class MsgId extends MsgIdBase {
  public static final MsgId EMPTY = new MsgId(0L, 0L);

  public MsgId(long hi, long lo) {
    super(hi, lo);
  }
}
