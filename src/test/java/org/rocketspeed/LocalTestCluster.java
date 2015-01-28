package org.rocketspeed;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.List;

public class LocalTestCluster implements AutoCloseable {

  private static final String ENV_RS_CLUSTER_BIN_PATH = "RS_CLUSTER_BIN_PATH";
  private static final int COMMAND_READY = 'R';
  private static final int COMMAND_QUIT = 'Q';
  private static final int PILOT_DEFAULT_PORT = 58600;
  private static final int COPILOT_DEFAULT_PORT = 58600;
  private static final int SHUTDOWN_RETRIES = 100;
  private Process cluster;
  private OutputStreamWriter clusterIn;
  private InputStreamReader clusterOut;

  private static String getBinaryPath() {
    return System.getenv(ENV_RS_CLUSTER_BIN_PATH);
  }

  public LocalTestCluster() throws IOException, InterruptedException {
    this(getBinaryPath());
  }

  public LocalTestCluster(String binaryPath) throws IOException, InterruptedException {
    try {
      if (binaryPath != null) {
        cluster = new ProcessBuilder(binaryPath).redirectError(ProcessBuilder.Redirect.INHERIT).
            start();
        clusterIn = new OutputStreamWriter(cluster.getOutputStream());
        clusterOut = new InputStreamReader(cluster.getInputStream());
        // Wait for cluster to start.
        if (COMMAND_READY != clusterOut.read()) {
          throw new IOException("Unexpected command.");
        }
      }
    } catch (IOException e) {
      close();
      throw e;
    }
  }

  public List<HostId> getPilots() {
    return Collections.singletonList(new HostId("localhost", PILOT_DEFAULT_PORT));
  }

  public List<HostId> getCopilots() {
    return Collections.singletonList(new HostId("localhost", COPILOT_DEFAULT_PORT));
  }

  public Configuration createConfiguration() {
    Configuration config = new Configuration();
    for (HostId hostId : getPilots()) {
      config.addPilot(hostId.getHostname(), hostId.getPort());
    }
    for (HostId hostId : getCopilots()) {
      config.addCopilot(hostId.getHostname(), hostId.getPort());
    }
    return config;
  }

  private boolean hasTerminated() {
    try {
      cluster.exitValue();
      return true;
    } catch (IllegalThreadStateException ignored) {
      return false;
    }
  }

  @Override
  public void close() throws InterruptedException, IOException {
    // Possible partial initialisation 1): failed starting cluster process.
    if (cluster != null) {
      // Try to cleanly shut down the cluster.
      clusterOut.close();
      clusterIn.write(COMMAND_QUIT);
      clusterIn.flush();
      clusterIn.close();
      // Wait for clean shutdown.
      int i;
      for (i = 0; i < SHUTDOWN_RETRIES; ++i) {
        if (hasTerminated()) {
          break;
        }
        Thread.sleep(5);
      }
      // If diplomacy fails, just kill it.
      if (i == SHUTDOWN_RETRIES) {
        cluster.destroy();
        cluster.waitFor();
      }
    }
    // Possible partial initialisation 2): failed to get right command.
  }
}
