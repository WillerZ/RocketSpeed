package org.rocketspeed;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class LocalTestCluster implements AutoCloseable {

  private static final int COMMAND_READY = 'R';
  private static final int COMMAND_QUIT = 'Q';
  private static final int PILOT_DEFAULT_PORT = 58600;
  private static final int COPILOT_DEFAULT_PORT = 58600;
  private static final int STARTUP_RETRIES = 200;
  private static final int SHUTDOWN_RETRIES = 100;
  private Process cluster;
  private OutputStreamWriter clusterIn;
  private InputStreamReader clusterOut;

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
        int i;
        for (i = 0; i < STARTUP_RETRIES; ++i) {
          if (clusterOut.ready()) {
            break;
          }
          Thread.sleep(5);
        }
        if (i == STARTUP_RETRIES) {
          throw new IOException("Cluster took too long to start.");
        }
        if (COMMAND_READY != clusterOut.read()) {
          throw new IOException("Unexpected command.");
        }
      }
    } catch (IOException e) {
      close();
      throw e;
    }
  }

  private static String getBinaryPath() {
    return "buck-out/gen/rocketspeed/github/src/test/test_cluster_proc";
  }

  public HostId getCockpit() {
    return new HostId("localhost", PILOT_DEFAULT_PORT);
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
