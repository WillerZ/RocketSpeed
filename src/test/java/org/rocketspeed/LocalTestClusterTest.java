package org.rocketspeed;

import org.junit.Test;

public class LocalTestClusterTest {

  @Test
  public void testCluster() throws Exception {
    try (LocalTestCluster cluster = new LocalTestCluster()) {
      cluster.getCockpit();
    }
  }
}
