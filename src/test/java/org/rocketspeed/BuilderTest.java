package org.rocketspeed;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BuilderTest {

  @Test
  public void testExceptionOnClientOpen() {
    Builder builder = new Builder().tenantID(23)
        .clientID("client-id-123")
        .configuration(new Configuration())
        .resubscribeFromStorage();
    try {
      builder.build();
    } catch (Exception e) {
      assertEquals("Invalid argument: TenantId must be greater than 100.", e.getMessage());
      return;
    }
    fail("Exception expected");
  }
}
