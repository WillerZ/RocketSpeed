package org.rocketspeed;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Proxy implements AutoCloseable {

  /* package */ static final Logger LOGGER = Logger.getLogger(Proxy.class.getName());

  static {
    System.loadLibrary("rocketspeedjni");
  }

  private final ProxyImpl proxyImpl;

  public Proxy(LogLevel debugLevel, Configuration config, int workersNumber) {
    proxyImpl = ProxyImpl.create(debugLevel, config.djinni(), workersNumber);
  }

  public void start(final MessageCallback messageCallback,
                    final DisconnectCallback disconnectCallback) throws Exception {
    MessageCallback messageCallback1 = new MessageCallback() {
      @Override
      public void call(long sessionId, byte[] message) {
        try {
          messageCallback.call(sessionId, message);
        } catch (Throwable e) {
          LOGGER.log(Level.WARNING, "Exception thrown in disconnect callback", e);
        }
      }
    };
    DisconnectCallback disconnectCallback1 = new DisconnectCallback() {
      @Override
      public void call(ArrayList<Long> sessionIds) {
        try {
          disconnectCallback.call(sessionIds);
        } catch (Throwable e) {
          LOGGER.log(Level.WARNING, "Exception thrown in disconnect callback", e);
        }
      }
    };
    // Now this is quite dangerous, as nothing prevents us from passing original callbacks, which
    // leads to native crash when someone throws an exception in any of the callback.
    // Fortunately unused variable warning will be triggered, but who reads warning messages?
    Status status = proxyImpl.start(messageCallback1, disconnectCallback1);
    status.checkExceptions();
  }

  public Status forward(byte[] message, long sessionId, int sequenceNumber) {
    return proxyImpl.forward(message, sessionId, sequenceNumber);
  }

  public void destroySession(long sessionId) {
    proxyImpl.destroySession(sessionId);
  }

  @Override
  public void close() {
    proxyImpl.close();
  }
}
