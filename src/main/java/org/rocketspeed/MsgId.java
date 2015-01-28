package org.rocketspeed;

import java.util.Arrays;

public class MsgId {

  private final MsgIdImpl impl;

  public MsgId() {
    impl = new MsgIdImpl(new byte[MsgIdImpl.SIZE]);
  }

  public MsgId(byte[] guid) {
    if (guid == null) {
      throw new NullPointerException();
    }
    if (guid.length != MsgIdImpl.SIZE) {
      throw new IllegalArgumentException("Invalid size of GUID");
    }
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
    return this == o || !(o == null || getClass() != o.getClass()) &&
                        Arrays.equals(impl.getGuid(), ((MsgId) o).impl.getGuid());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(impl.getGuid());
  }
}
