package org.rocketspeed;

public class RocketeerServer implements AutoCloseable {

  static {
    System.loadLibrary("rsjni");
  }

  private final RocketeerServerImpl rocketeerServer;

  public RocketeerServer(LogLevel logLevel, int listenerPort) {
    rocketeerServer = RocketeerServerImpl.create(logLevel, listenerPort);
  }

  public void register(Rocketeer rocketeer) {
    rocketeerServer.register(rocketeer);
  }

  public void start() throws Exception {
    rocketeerServer.start().checkExceptions();
  }

  public boolean deliver(InboundID inboundId, long seqno, byte[] payload) {
    if (seqno < 0) {
      throw new IllegalArgumentException("Sequence number must be non-negative");
    }
    return rocketeerServer.deliver(inboundId, seqno, payload);
  }

  public boolean terminate(InboundID inboundId) {
    return rocketeerServer.terminate(inboundId);
  }

  @Override
  public void close() throws Exception {
    rocketeerServer.close();
  }
}
