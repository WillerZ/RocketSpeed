package org.rocketspeed.android;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.rocketspeed.Builder;
import org.rocketspeed.Client;
import org.rocketspeed.LocalTestCluster;
import org.rocketspeed.MessageReceived;
import org.rocketspeed.MsgId;
import org.rocketspeed.PublishCallback;
import org.rocketspeed.ReceiveCallback;
import org.rocketspeed.Retention;
import org.rocketspeed.Status;
import org.rocketspeed.SubscribeCallback;
import org.rocketspeed.SubscriptionRequest;
import org.rocketspeed.TopicOptions;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.rocketspeed.SubscriptionStart.CURRENT;

public class WakeLockTest {

  public static final long TIMEOUT = 5L;
  public static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;
  private static LocalTestCluster testCluster;
  private WakeLock wakeLock;
  private InOrder order;
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
    order = inOrder(wakeLock);
    calledBack = new Semaphore(0);
    publishCallback = new PublishCallback() {
      @Override
      public void call(Status status, int namespaceId, String topicName, MsgId messageId,
                       long sequenceNumber) {
        // We sent a message.
        order.verify(wakeLock).acquire(eq(1000L));
        // And received a response.
        order.verify(wakeLock).acquire(eq(500L));
        calledBack.release();
      }
    };

    ReceiveCallback receiveCallback = new ReceiveCallback() {
      @Override
      public void call(MessageReceived message) {
        order.verify(wakeLock).acquire(eq(500L));
        calledBack.release();
      }
    };
    SubscribeCallback subscribeCallback = new SubscribeCallback() {
      @Override
      public void call(Status status, long sequenceNumber, boolean subscribed) {
        // We sent a message.
        order.verify(wakeLock).acquire(eq(1000L));
        // And received a response.
        order.verify(wakeLock).acquire(eq(500L));
        calledBack.release();
      }
    };
    client = new Builder(wakeLock).clientID("client-id-123")
        .configuration(testCluster.createConfiguration())
        .tenantID(123)
        .receiveCallback(receiveCallback)
        .subscribeCallback(subscribeCallback)
        .build();
  }

  @After
  public void tearDown() throws Exception {
    client.close();
  }

  @Test
  public void testLockedInCallbacks() throws Exception {
    int ns = 101;
    String topic = "test_topic-LockedInCallbacks";

    // Subscribe to a topic that we will be publishing to.
    client.listenTopics(asList(new SubscriptionRequest(ns, topic, true, CURRENT)));
    // Wait for confirmation.
    assertTrue(calledBack.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

    // Publish to the topic.
    client.publish(ns, topic, new TopicOptions(Retention.ONE_DAY), new byte[3], publishCallback);
    // Wait for confirmation...
    assertTrue(calledBack.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
    // ...and message.
    assertTrue(calledBack.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
  }
}
