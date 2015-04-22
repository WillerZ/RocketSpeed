package org.rocketspeed;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BuilderTest {

  @Test
  public void testExceptionOnClientOpen() {
    Builder builder = new Builder().resubscribeFromStorage();
    try {
      builder.build();
    } catch (Exception e) {
      assertEquals("Missing Configuration.", e.getMessage());
      return;
    }
    fail("Exception expected");
  }
}
