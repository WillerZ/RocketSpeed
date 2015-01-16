package org.rocketspeed;

import java.util.Arrays;

public class MsgId {

  private final MsgIdImpl impl;

  public MsgId(byte[] guid) {
    impl = new MsgIdImpl(guid);
  }

  /* package */ MsgId(MsgIdImpl messageId) {
    impl = messageId;
  }

  /* package */ MsgIdImpl djinni() {
    return impl;
  }

  @Override
  public boolean equals(Object o) {
    return this == o || !(o == null || getClass() != o.getClass()) && Arrays.equals(impl.getGuid(),
                                                                                    ((MsgId) o).impl
                                                                                        .getGuid());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(impl.getGuid());
  }
}
