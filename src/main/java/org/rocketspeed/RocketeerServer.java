package org.rocketspeed;

import java.util.HashMap;
import java.util.Map;

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

  public void register(final RocketeerWithParams rocketeer) {
    rocketeerServer.register(
        new Rocketeer() {
          private final Map<InboundID, SubscriptionParameters> activeSubscriptions =
              new HashMap<InboundID, SubscriptionParameters>();

          @Override
          public void handleNewSubscription(
              InboundID inboundId, SubscriptionParameters params) {
            activeSubscriptions.put(inboundId, params);
            rocketeer.handleNewSubscription(inboundId, params);
          }

          @Override
          public void handleTermination(InboundID inboundId, boolean fromClient) {
            SubscriptionParameters params = activeSubscriptions.remove(inboundId);
            if (!fromClient) {
              return;
            }
            if (params == null) {
              throw new IllegalStateException("Missing subscription parameters");
            }
            rocketeer.handleTermination(inboundId, params);
          }
        });
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
