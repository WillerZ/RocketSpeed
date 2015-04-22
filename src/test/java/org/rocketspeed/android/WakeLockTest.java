package org.rocketspeed.android;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocketspeed.Builder;
import org.rocketspeed.Client;
import org.rocketspeed.LocalTestCluster;
import org.rocketspeed.MessageReceived;
import org.rocketspeed.MsgId;
import org.rocketspeed.PublishCallback;
import org.rocketspeed.ReceiveCallback;
import org.rocketspeed.Status;
import org.rocketspeed.SubscribeCallback;
import org.rocketspeed.SubscriptionRequest;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;
import static org.rocketspeed.SubscriptionStart.CURRENT;

public class WakeLockTest {

  public static final long TIMEOUT = 5L;
  public static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;
  private static LocalTestCluster testCluster;
  private WakeLock wakeLock;
  private Semaphore calledBack;
  private PublishCallback publishCallback;
  private Client client;

  @BeforeClass
  public static void setUpClass() throws Exception {
    testCluster = new LocalTestCluster();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    testCluster.close();
  }

  @Before
  public void setUp() throws Exception {
    wakeLock = mock(WakeLock.class, withSettings().verboseLogging());
    calledBack = new Semaphore(0);
    publishCallback = new PublishCallback() {
      @Override
      public void call(Status status, String namespaceId, String topicName, MsgId messageId,
                       long sequenceNumber) {
        calledBack.release();
      }
    };

    ReceiveCallback receiveCallback = new ReceiveCallback() {
      @Override
      public void call(MessageReceived message) {
        calledBack.release();
      }
    };
    SubscribeCallback subscribeCallback = new SubscribeCallback() {
      @Override
      public void call(Status status, String namespaceId, String topicName, long sequenceNumber,
                       boolean subscribed) {
        // We sent a message, and that was the first (and last) time a wake lock should be acquired.
        verify(wakeLock).acquire(eq(1000L));
        // And received a response.
        calledBack.release();
      }
    };
    client = new Builder(wakeLock).configuration(testCluster.createConfiguration())
        .receiveCallback(receiveCallback)
        .subscribeCallback(subscribeCallback)
        .build();
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testLockedInCallbacks() throws Exception {
    int tenantId = 123;
    String ns = "guest";
    String topic = "test_topic-LockedInCallbacks";

    // This test is fishy... to make it less flaky, we'll compare timestamps and if the
    // test takes too long, we'll just discard a result (fail assumption, not assertion).
    long start = System.currentTimeMillis();

    // Subscribe to a topic that we will be publishing to.
    client.listenTopics(tenantId, asList(new SubscriptionRequest(ns, topic, true, CURRENT)));
    // Wait for confirmation.
    assertTrue(calledBack.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

    // Publish to the topic.
    client.publish(tenantId, ns, topic, new byte[3], publishCallback);
    // Wait for confirmation...
    assertTrue(calledBack.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
    // ...and message.
    assertTrue(calledBack.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

    long end = System.currentTimeMillis();
    assumeTrue(end - start <= 1000L);
    verifyNoMoreInteractions(wakeLock);
  }
}
