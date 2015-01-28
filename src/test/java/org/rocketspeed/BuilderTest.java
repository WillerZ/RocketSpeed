package org.rocketspeed;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BuilderTest {

  @Test
  public void testExceptionOnClientOpen() {
    Configuration config = new Configuration();
    // The configuration has no pilots or copilots, creation of a client must fail.
    Builder builder = new Builder().tenantID(123).clientID("client-id-123").configuration(config);
    try {
      builder.build();
    } catch (Exception e) {
      assertEquals("Invalid argument: Must have at least one pilot.", e.getMessage());
      return;
    }
    fail("Exception expected");
  }
}
