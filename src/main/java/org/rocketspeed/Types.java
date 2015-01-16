package org.rocketspeed;

/* package */ final class Types {

  private Types() {
  }

  public static short fromUnsignedShort(int namespaceId) {
    short namespaceId1 = (short) namespaceId;
    if (namespaceId != namespaceId1) {
      throw new IllegalArgumentException("Namespace id outside of range");
    }
    return (short) (namespaceId1 - Short.MIN_VALUE);
  }

  public static int toUnsignedShort(short namespaceId) {
    return namespaceId + Short.MIN_VALUE;
  }
}
