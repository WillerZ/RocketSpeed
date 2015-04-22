package org.rocketspeed;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
  public TemporaryFolder testFolder;

  @Before
  public void setUp() throws Exception {
    testFolder = new TemporaryFolder();
    testFolder.create();
    testCluster = new LocalTestCluster();
  }

  @After
  public void tearDown() throws Exception {
    if (testCluster != null) {
      testCluster.close();
    }
    if (testFolder != null) {
      testFolder.delete();
    }
  }

  @Test
  public void testOneMessage() throws Exception {
    // Message setup.
    final String ns = "guest";
    final String topic = "test_topic-OneMessage";
    final byte[] data = "test_data-OneMessage".getBytes();

    // For asserting that all async callbacks carried successful statuses.
    final StatusMonoidFirst statuses = new StatusMonoidFirst();

    // Callbacks.
    final Semaphore publishSemaphore = new Semaphore(0);
    PublishCallback publishCallback = new PublishCallback() {
      @Override
      public void call(Status status, String namespaceId, String topicName, MsgId messageId,
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
      public void call(Status status, String namespaceId, String topicName, long sequenceNumber,
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
    Builder builder = new Builder().configuration(testCluster.createConfiguration())
        .receiveCallback(receiveCallback)
        .subscribeCallback(subscribeCallback);

    try (Client client = builder.build()) {
      // Send a message.
      client.publish(tenantId, ns, topic, data, publishCallback);

      // Listen for the topic from the beginning of time.
      client.listenTopics(tenantId,
                          singletonList(new SubscriptionRequest(ns, topic, true, BEGINNING)));

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
    final String ns = "guest";
    final String topic = "test_topic-SequenceNumberZero";

    // For asserting that all async callbacks carried successful statuses.
    final StatusMonoidFirst statuses = new StatusMonoidFirst();

    // Callbacks.
    final Semaphore publishSemaphore = new Semaphore(0);
    PublishCallback publishCallback = new PublishCallback() {
      @Override
      public void call(Status status, String namespaceId, String topicName, MsgId messageId,
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
      public void call(Status status, String namespaceId, String topicName, long sequenceNumber,
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
    Builder builder = new Builder().configuration(testCluster.createConfiguration())
        .receiveCallback(receiveCallback)
        .subscribeCallback(subscribeCallback);

    try (Client client = builder.build()) {
      // Send some messages and wait for the ACKs.
      for (int i = 1; i < 4; ++i) {
        client.publish(tenantId, ns, topic, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      // Subscribe starting from current topic head.
      client.listenTopics(tenantId,
                          singletonList(new SubscriptionRequest(ns, topic, true, CURRENT)));
      assertTrue(subscribeSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

      // Send 3 more different messages.
      for (int i = 3; i < 6; ++i) {
        client.publish(tenantId, ns, topic, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      // Should not receive any of the first three messages.
      assertEquals(asList("3", "4", "5"), receivedMessages);

      // Unsubscribe.
      client.listenTopics(tenantId,
                          singletonList(new SubscriptionRequest(ns, topic, false, UNKNOWN)));
      assertTrue(subscribeSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

      // Send some messages and wait for the ACKs.
      for (int i = 6; i < 9; ++i) {
        client.publish(tenantId, ns, topic, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      // Subscribe starting from current topic head.
      client.listenTopics(tenantId,
                          singletonList(new SubscriptionRequest(ns, topic, true, CURRENT)));
      assertTrue(subscribeSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

      // Send 3 more messages again.
      for (int i = 9; i < 12; ++i) {
        client.publish(tenantId, ns, topic, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      // Should not receive any of the messages sent while we were not subscribing.
      assertEquals(asList("3", "4", "5", "9", "10", "11"), receivedMessages);
    }

    // Assert that all statuses were OK.
    statuses.checkExceptions();
  }

  @Test
  public void testWithSubscriptionStorage() throws Exception {
    // Message setup.
    final String ns = "guest";
    final String topic = "test_topic-ResubscribeFromStorage";

    // For asserting that all async callbacks carried successful statuses.
    final StatusMonoidFirst statuses = new StatusMonoidFirst();

    // Callbacks.
    final Semaphore receiveSemaphore = new Semaphore(0);
    final List<String> receivedMessages = synchronizedList(new ArrayList<String>());
    ReceiveCallback receiveCallback = new ReceiveCallback() {
      @Override
      public void call(MessageReceived message) {
        assertEquals(ns, message.getNamespaceId());
        assertEquals(topic, message.getTopicName());
        message.acknowledge();
        receivedMessages.add(new String(message.getContents()));
        receiveSemaphore.release();
      }
    };
    final Semaphore snapshotSemaphore = new Semaphore(0);
    SnapshotCallback snapshotCallback = new SnapshotCallback() {
      @Override
      public void call(Status status) {
        statuses.append(status);
        snapshotSemaphore.release();
      }
    };

    // Client setup.
    String clientId = "test_topic-ResubscribeFromStorage";
    int tenantId = 102;
    String snapshotFile = testFolder.getRoot().getAbsolutePath() + "/ResubscribeFromStorage.bin";
    Builder builder = new Builder().configuration(testCluster.createConfiguration());

    // Create a client.
    try (Client client = builder.receiveCallback(receiveCallback)
        .usingFileStorage(snapshotFile, false)
        .build()) {
      // Subscribe starting from the first sequence number.
      client.listenTopics(tenantId,
                          singletonList(new SubscriptionRequest(ns, topic, true, BEGINNING)));
      // Send some messages and wait for the ACKs.
      for (int i = 0; i < 3; ++i) {
        client.publish(tenantId, ns, topic, valueOf(i).getBytes());
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }
      // Should receive all of the first three messages.
      assertEquals(asList("0", "1", "2"), receivedMessages);
      // Save snapshot and wait for it to succeed.
      client.saveSubscriptions(snapshotCallback);
      assertTrue(snapshotSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      // Destroy the client.
    }
    // Assert that all statuses were OK.
    statuses.checkExceptions();
    // Clear received messages.
    receivedMessages.clear();

    // Create a client again.
    try (Client client = builder.receiveCallback(receiveCallback)
        .usingFileStorage(snapshotFile, true)
        .build()) {
      // Subscribe starting from unknown sequence number, a subscription storage is expected to
      // provide this information.
      client.listenTopics(tenantId,
                          singletonList(new SubscriptionRequest(ns, topic, true, UNKNOWN)));
      // Send some different messages and wait for the ACKs.
      for (int i = 3; i < 6; ++i) {
        client.publish(tenantId, ns, topic, valueOf(i).getBytes());
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }
      // Should receive none of the first three messages and all from the second batch.
      assertEquals(asList("3", "4", "5"), receivedMessages);
      // Save snapshot and wait for it to succeed.
      client.saveSubscriptions(snapshotCallback);
      assertTrue(snapshotSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
    }
    // Assert that all statuses were OK.
    statuses.checkExceptions();
    // Clear received messages.
    receivedMessages.clear();

    // Create a client once again.
    try (Client client = builder.receiveCallback(receiveCallback)
        .usingFileStorage(snapshotFile, true)
        .resubscribeFromStorage()
        .build()) {
      // We will not resubscribe explicitly this time, subscription storage should do it
      // automatically.
      // Send some different messages and wait for the ACKs.
      for (int i = 6; i < 9; ++i) {
        client.publish(tenantId, ns, topic, valueOf(i).getBytes());
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }
      // Should receive none of the first three messages and all from the second batch.
      assertEquals(asList("6", "7", "8"), receivedMessages);
      // Save snapshot and wait for it to succeed.
      client.saveSubscriptions(snapshotCallback);
      assertTrue(snapshotSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
    }
    // Assert that all statuses were OK.
    statuses.checkExceptions();
    // Clear received messages.
    receivedMessages.clear();
  }
}
