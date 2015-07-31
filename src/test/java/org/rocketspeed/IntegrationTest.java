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
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.*;
import static org.rocketspeed.Client.*;

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
    final String topic = "OneMessage";
    final byte[] data = "OneMessage:data".getBytes();
    final StatusMonoidFirst statuses = new StatusMonoidFirst();

    final Semaphore publishSemaphore = new Semaphore(0);
    PublishCallback publishCallback = new PublishCallback() {
      @Override
      public void call(
          MsgId messageId, String namespaceId, String topicName, long seqno, Status status) {
        assertEquals(GUEST_NAMESPACE, namespaceId);
        assertEquals(topic, topicName);
        statuses.append(status);
        publishSemaphore.release();
        // This exception should be swallowed by a thread that executes the callback.
        throw new RuntimeException("Catch me if you can!");
      }
    };
    final Semaphore receiveSemaphore = new Semaphore(0);
    MessageReceivedCallback receiveCallback = new MessageReceivedCallback() {
      @Override
      public void call(long subHandle, long startSeqno, byte[] contents) {
        assertArrayEquals(data, contents);
        receiveSemaphore.release();
        // This exception should be swallowed by a thread that executes the callback.
        throw new RuntimeException("Catch me if you can!");
      }
    };

    try (Client client = new Builder().cockpit(testCluster.getCockpit()).build()) {
      client.publish(GUEST_TENANT, GUEST_NAMESPACE, topic, data, publishCallback);
      client.subscribe(GUEST_TENANT, GUEST_NAMESPACE, topic, BEGINNING_SEQNO, receiveCallback);
      assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
    }

    statuses.checkExceptions();
  }

  @Test
  public void testSequenceNumberZero() throws Exception {
    final String topic = "SequenceNumberZero";
    final StatusMonoidFirst statuses = new StatusMonoidFirst();

    final Semaphore publishSemaphore = new Semaphore(0);
    PublishCallback publishCallback = new PublishCallback() {
      @Override
      public void call(
          MsgId messageId, String namespaceId, String topicName, long seqno, Status status) {
        assertEquals(GUEST_NAMESPACE, namespaceId);
        assertEquals(topic, topicName);
        statuses.append(status);
        publishSemaphore.release();
      }
    };
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
        assertEquals(GUEST_TENANT, tenantId);
        assertEquals(GUEST_NAMESPACE, namespaceId);
        assertEquals(topic, topicName);
        subscribeSemaphore.release();
      }
    };
    final Semaphore receiveSemaphore = new Semaphore(0);
    final List<String> receivedMessages = synchronizedList(new ArrayList<String>());
    MessageReceivedCallback receiveCallback = new MessageReceivedCallback() {
      @Override
      public void call(long subHandle, long startSeqno, byte[] contents) {
        receivedMessages.add(new String(contents));
        receiveSemaphore.release();
      }
    };

    try (Client client = new Builder().cockpit(testCluster.getCockpit()).build()) {
      for (int i = 1; i < 4; ++i) {
        client.publish(
            GUEST_TENANT, GUEST_NAMESPACE, topic, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      long handle0 = client.subscribe(
          GUEST_TENANT, GUEST_NAMESPACE, topic, CURRENT_SEQNO, receiveCallback, subscribeCallback);
      Thread.sleep(300);

      for (int i = 3; i < 6; ++i) {
        client.publish(
            GUEST_TENANT, GUEST_NAMESPACE, topic, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      assertEquals(asList("3", "4", "5"), receivedMessages);

      client.unsubscribe(handle0);
      assertTrue(subscribeSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

      for (int i = 6; i < 9; ++i) {
        client.publish(
            GUEST_TENANT, GUEST_NAMESPACE, topic, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      client.subscribe(GUEST_TENANT, GUEST_NAMESPACE, topic, CURRENT_SEQNO, receiveCallback);
      Thread.sleep(300);

      for (int i = 9; i < 12; ++i) {
        client.publish(
            GUEST_TENANT, GUEST_NAMESPACE, topic, valueOf(i).getBytes(), publishCallback);
        assertTrue(publishSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
        assertTrue(receiveSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));
      }

      // Should not receive any of the messages sent while we were not subscribing.
      assertEquals(asList("3", "4", "5", "9", "10", "11"), receivedMessages);
    }

    statuses.checkExceptions();
  }

  @Test
  public void testSubscriptionStorage() throws Exception {
    final StatusMonoidFirst statuses = new StatusMonoidFirst();

    final Semaphore snapshotSemaphore = new Semaphore(0);
    SnapshotCallback snapshotCallback = new SnapshotCallback() {
      @Override
      public void call(Status status) {
        statuses.append(status);
        snapshotSemaphore.release();
      }
    };

    String snapshotFile = testFolder.getRoot().getAbsolutePath() + "/SubscriptionStorage.bin";
    try (Client client = new Builder().cockpit(testCluster.getCockpit()).usingFileStorage(
        snapshotFile).build()) {
      SubscriptionParameters params = new SubscriptionParameters(
          GUEST_TENANT, GUEST_NAMESPACE, "SubscriptionStorage", 123L);
      long handle0 = client.subscribe(params, null);
      client.acknowledge(handle0, 125L);

      client.saveSubscriptions(snapshotCallback);
      assertTrue(snapshotSemaphore.tryAcquire(TIMEOUT, TIMEOUT_UNIT));

      ArrayList<SubscriptionParameters> list = client.restoreSubscriptions();
      assertEquals(1, list.size());
      SubscriptionParameters restored = list.get(0);
      assertEquals(params.getTenantId(), restored.getTenantId());
      assertEquals(params.getNamespaceId(), restored.getNamespaceId());
      assertEquals(params.getTopicName(), restored.getTopicName());
      assertEquals(126L, restored.getStartSeqno());
    }

    statuses.checkExceptions();
  }
}
