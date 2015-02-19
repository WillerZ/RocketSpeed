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
      public void call(String clientId, byte[] message) {
        try {
          messageCallback.call(clientId, message);
        } catch (Throwable e) {
          LOGGER.log(Level.WARNING, "Exception thrown in disconnect callback", e);
        }
      }
    };
    DisconnectCallback disconnectCallback1 = new DisconnectCallback() {
      @Override
      public void call(ArrayList<String> clientIds) {
        try {
          disconnectCallback.call(clientIds);
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

  public void forward(byte[] message, long sessionId, int sequenceNumber) throws Exception {
    Status status = proxyImpl.forward(message, sessionId, sequenceNumber);
    status.checkExceptions();
  }

  public void destroySession(long sessionId) throws Exception {
    proxyImpl.destroySession(sessionId);
  }

  @Override
  public void close() {
    proxyImpl.close();
  }
}
