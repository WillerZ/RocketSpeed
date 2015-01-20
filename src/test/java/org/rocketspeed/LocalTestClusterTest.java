package org.rocketspeed;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LocalTestClusterTest {

  @Test
  public void testCluster() throws Exception {
    try (LocalTestCluster cluster = new LocalTestCluster()) {
      assertFalse(cluster.getPilots().isEmpty());
      assertFalse(cluster.getCopilots().isEmpty());
      Configuration config = cluster.createConfiguration(101);
      assertEquals(101, config.djinni().getTenantId());
      assertEquals(cluster.getPilots(), config.djinni().getPilots());
      assertEquals(cluster.getCopilots(), config.djinni().getCopilots());
    }
  }
}
