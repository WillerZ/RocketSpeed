package org.rocketspeed;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocketspeed.util.StatusMonoidFirst;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.rocketspeed.SubscriptionStart.BEGINNING;
import static org.rocketspeed.SubscriptionStart.CURRENT;
import static org.rocketspeed.SubscriptionStart.UNKNOWN;

public class IntegrationTest {

  public static final long TIMEOUT = 5L;
  public static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;
  public LocalTestCluster testCluster;

  @Before
  public void setUp() throws Exception {
    testCluster = new LocalTestCluster();
  }

  @After
  public void tearDown() throws Exception {
    testCluster.close();
  }

  @Test
  public void testOneMessage() throws Exception {
    // Message setup.
    final int ns = 101;
    final String topic = "test_topic-OneMessage";
    final byte[] data = "test_data-OneMessage".getBytes();

    // For asserting that all async callbacks carried successful statuses.
    final StatusMonoidFirst statuses = new StatusMonoidFirst();

    // Callbacks.
    final Semaphore publishSemaphore = new Semaphore(0);
    PublishCallback publishCallback = new PublishCallback() {
      @Override
      public void call(Status status, int namespaceId, String topicName, MsgId messageId,
                       long sequenceNumber) {
        assertEquals(ns, namespaceId);
        assertEquals(topic, topicName);
        statuses.append(status);
        publishSemaphore.release();
        // This exception should be swallowed by a thread that executes the callback.
        throw new RuntimeException("Catch me if you can!");
      }
    };
    final Semaphore subscribeSemaphore = new Semaphore(0);
    SubscribeCallback subscribeCallback = new SubscribeCallback() {
      @Override
      public void call(Status status, int namespaceId, String topicName, long sequenceNumber,
                       boolean subscribed) {
        assertEquals(ns, namespaceId);
        assertEquals(topic, topicName);
        assertEquals(true, subscribed);
        statuses.append(status);
        subscribeSemaphore.release();
        // This exception should be swallowed by a thread that executes the callback.
        throw new RuntimeException("Catch me if you can!");
      }
    };
    final Semaphore receiveSemaphore = new Semaphore(0);
    ReceiveCallback receiveCallback = new ReceiveCallback() {
      @Override
      public void call(MessageReceived message) {
        assertEquals(ns, message.getNamespaceId());
        assertEquals(topic, message.getTopicName());
        assertArrayEquals(data, message.getContents());
        receiveSemaphore.release();
        // This exception should be swallowed by a thread that executes the callback.
        throw new RuntimeException("Catch me if you can!");
      }
    };

    // Client setup.
    String clientId = "test_client_id-OneMessage";
    int tenantId = 101;
    Builder builder = new Builder().clientID(clientId)
        .configuration(testCluster.createConfiguration())
        .tenantID(tenantId)
        .receiveCallback(receiveCallback)
        .subscribeCallback(subscribeCallback);

    try (Client client = builder.build()) {
      // Send a message.
      client.publish(ns, topic, new TopicOptions(Retention.ONE_DAY), data, publishCallback);

      // Listen for the topic from the beginning of time.
      client.listenTopics(singletonList(new SubscriptionRequest(ns, topic, true, BEGINNING)));

      // Wait for publish ack.
      assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      // Wait for subscribe ack.
      assertTrue(subscribeSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      // Wait for the message.
      assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
    }

    // Assert that all statuses were OK.
    statuses.checkExceptions();
  }

  @Test
  public void testSequenceNumberZero() throws Exception {
    // Message setup.
    final int ns = 102;
    final String topic = "test_topic-SequenceNumberZero";
    final TopicOptions options = new TopicOptions(Retention.ONE_DAY);

    // For asserting that all async callbacks carried successful statuses.
    final StatusMonoidFirst statuses = new StatusMonoidFirst();

    // Callbacks.
    final Semaphore publishSemaphore = new Semaphore(0);
    PublishCallback publishCallback = new PublishCallback() {
      @Override
      public void call(Status status, int namespaceId, String topicName, MsgId messageId,
                       long sequenceNumber) {
        assertEquals(ns, namespaceId);
        assertEquals(topic, topicName);
        statuses.append(status);
        publishSemaphore.release();
      }
    };
    final Semaphore subscribeSemaphore = new Semaphore(0);
    SubscribeCallback subscribeCallback = new SubscribeCallback() {
      @Override
      public void call(Status status, int namespaceId, String topicName, long sequenceNumber,
                       boolean subscribed) {
        assertEquals(ns, namespaceId);
        assertEquals(topic, topicName);
        statuses.append(status);
        subscribeSemaphore.release();
      }
    };
    final Semaphore receiveSemaphore = new Semaphore(0);
    final List<String> receivedMessages = synchronizedList(new ArrayList<String>());
    ReceiveCallback receiveCallback = new ReceiveCallback() {
      @Override
      public void call(MessageReceived message) {
        assertEquals(ns, message.getNamespaceId());
        assertEquals(topic, message.getTopicName());
        receivedMessages.add(new String(message.getContents()));
        receiveSemaphore.release();
      }
    };

    // Client setup.
    String clientId = "test_topic-SequenceNumberZero";
    int tenantId = 102;
    Builder builder = new Builder().clientID(clientId)
        .configuration(testCluster.createConfiguration())
        .tenantID(tenantId)
        .receiveCallback(receiveCallback)
        .subscribeCallback(subscribeCallback);

    try (Client client = builder.build()) {
      // Send some messages and wait for the ACKs.
      for (int i = 1; i < 4; ++i) {
        client.publish(ns, topic, options, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      // Subscribe starting from current topic head.
      client.listenTopics(singletonList(new SubscriptionRequest(ns, topic, true, CURRENT)));
      assertTrue(subscribeSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

      // Send 3 more different messages.
      for (int i = 3; i < 6; ++i) {
        client.publish(ns, topic, options, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      // Should not receive any of the first three messages.
      assertEquals(asList("3", "4", "5"), receivedMessages);

      // Unsubscribe.
      client.listenTopics(singletonList(new SubscriptionRequest(ns, topic, false, UNKNOWN)));
      assertTrue(subscribeSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

      // Send some messages and wait for the ACKs.
      for (int i = 6; i < 9; ++i) {
        client.publish(ns, topic, options, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      // Subscribe starting from current topic head.
      client.listenTopics(singletonList(new SubscriptionRequest(ns, topic, true, CURRENT)));
      assertTrue(subscribeSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

      // Send 3 more messages again.
      for (int i = 9; i < 12; ++i) {
        client.publish(ns, topic, options, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      // Should not receive any of the messages sent while we were not subscribing.
      assertEquals(asList("3", "4", "5", "9", "10", "11"), receivedMessages);
    }

    // Assert that all statuses were OK.
    statuses.checkExceptions();
  }
}
