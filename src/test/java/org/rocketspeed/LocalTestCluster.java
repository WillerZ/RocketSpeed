package org.rocketspeed;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LocalTestCluster implements AutoCloseable {

  private static final String ENV_ROCKETSPEED_BINARY_PATH = "ROCKETSPEED_BINARY_PATH";
  private static final int PILOT_DEFAULT_PORT = 58600;
  private static final int COPILOT_DEFAULT_PORT = 58600;
  private final String binaryPath;
  private Process cluster;

  private static String getRocketspeedBinaryPath() {
    return System.getenv(ENV_ROCKETSPEED_BINARY_PATH);
  }

  public LocalTestCluster() throws IOException, InterruptedException {
    this(getRocketspeedBinaryPath());
  }

  public LocalTestCluster(String binaryPath) throws IOException, InterruptedException {
    this.binaryPath = binaryPath;
    if (binaryPath != null) {
      cluster = new ProcessBuilder(binaryPath).inheritIO().start();
      // This is rather suboptimal. In the future we can make a wrapper over single process cluster
      // to communicate pilots/copilots ports and asynchronous events via stdout.
      Thread.sleep(500);
    }
  }

  public List<HostId> getPilots() {
    return Collections.singletonList(new HostId("localhost", PILOT_DEFAULT_PORT));
  }

  public List<HostId> getCopilots() {
    return Collections.singletonList(new HostId("localhost", COPILOT_DEFAULT_PORT));
  }

  public Configuration createConfiguration(int tenantId) {
    Configuration config = new Configuration(tenantId);
    for (HostId hostId : getPilots()) {
      config.addPilot(hostId.getHostname(), hostId.getPort());
    }
    for (HostId hostId : getCopilots()) {
      config.addCopilot(hostId.getHostname(), hostId.getPort());
    }
    return config;
  }

  @Override
  public void close() throws InterruptedException {
    if (binaryPath != null) {
      try {
        cluster.exitValue();
        throw new IllegalStateException("Cluster process terminated prematurely.");
      } catch (IllegalThreadStateException ignored) {
        // It is expected that the cluster process still runs.
      }
      cluster.destroy();
      cluster.waitFor();
    }
  }
}
