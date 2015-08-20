package org.rocketspeed;

import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.rocketspeed.Client.GUEST_NAMESPACE;
import static org.rocketspeed.Client.GUEST_TENANT;
import static org.rocketspeed.LogLevel.DEBUG_LEVEL;

public class RocketeerTest {

  private static final long TIMEOUT = 5L;
  private static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;
  private static final int ROCKETEER_PORT = 58273;

  @Test
  public void testRocketeerSimple() throws Exception {
    final long nextSeqno = 123L;
    final byte[] payload = "abcdf".getBytes();
    final SubscriptionParameters parameters = new SubscriptionParameters(
        GUEST_TENANT, GUEST_NAMESPACE, "RocketeerSimple", nextSeqno - 42);

    try (final RocketeerServer rocketeerServer = new RocketeerServer(DEBUG_LEVEL, ROCKETEER_PORT)) {
      final Semaphore newSubscriptionSemaphore = new Semaphore(0);
      final Semaphore terminateSemaphore = new Semaphore(0);
      rocketeerServer.register(
          new RocketeerWithParams() {
            InboundID inboundID;

            @Override
            public void handleNewSubscription(InboundID inboundId, SubscriptionParameters params) {
              assertEquals(parameters.getTenantId(), params.getTenantId());
              assertEquals(parameters.getNamespaceId(), params.getNamespaceId());
              assertEquals(parameters.getTopicName(), params.getTopicName());
              assertEquals(parameters.getStartSeqno(), params.getStartSeqno());
              assertNull(this.inboundID);
              this.inboundID = inboundId;
              newSubscriptionSemaphore.release();
              assertTrue(rocketeerServer.deliver(inboundId, nextSeqno, payload));
            }

            @Override
            public void handleTermination(InboundID inboundId, SubscriptionParameters params) {
              assertEquals(parameters.getTenantId(), params.getTenantId());
              assertEquals(parameters.getNamespaceId(), params.getNamespaceId());
              assertEquals(parameters.getTopicName(), params.getTopicName());
              assertEquals(parameters.getStartSeqno(), params.getStartSeqno());
              assertNotNull(this.inboundID);
              assertEquals(this.inboundID, inboundId);
              this.inboundID = null;
              terminateSemaphore.release();
            }
          });
      rocketeerServer.start();

      final Semaphore subscribeSemaphore = new Semaphore(0);
      SubscribeCallback subscribeCallback = new SubscribeCallback() {
        @Override
        public void call(
            int tenantId,
            String namespaceId,
            String topicName,
            long startSeqno,
            boolean subscribed,
            Status status) {
          assertEquals(parameters.getTenantId(), tenantId);
          assertEquals(parameters.getNamespaceId(), namespaceId);
          assertEquals(parameters.getTopicName(), topicName);
          subscribeSemaphore.release();
        }
      };
      final Semaphore receiveSemaphore = new Semaphore(0);
      MessageReceivedCallback receiveCallback = new MessageReceivedCallback() {
        @Override
        public void call(long subHandle, long seqno, byte[] contents) {
          assertEquals(nextSeqno, seqno);
          assertArrayEquals(payload, contents);
          receiveSemaphore.release();
        }
      };

      Builder builder =
          new Builder().logLevel(DEBUG_LEVEL).cockpit(new HostId("localhost", ROCKETEER_PORT));
      try (Client client = builder.build()) {
        long handle0 = client.subscribe(
            parameters.getTenantId(),
            parameters.getNamespaceId(),
            parameters.getTopicName(),
            parameters.getStartSeqno(),
            receiveCallback,
            subscribeCallback);
        assertTrue(newSubscriptionSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
        client.unsubscribe(handle0);
        assertTrue(terminateSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
        assertTrue(subscribeSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }
    }
  }
}
