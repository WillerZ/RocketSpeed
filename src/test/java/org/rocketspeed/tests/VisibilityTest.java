package org.rocketspeed.tests;

import org.junit.Test;
import org.rocketspeed.Builder;
import org.rocketspeed.Configuration;
import org.rocketspeed.MessageReceived;
import org.rocketspeed.ReceiveCallback;
import org.rocketspeed.Status;
import org.rocketspeed.SubscribeCallback;

/**
 * This test is deliberately placed in a separate package, so that we can verify visibility of user
 * facing types.
 */
public class VisibilityTest {

  @Test
  public void testBuilder() throws Exception {
    Configuration config = new Configuration(123);
    Builder builder = new Builder().configuration(config)
        .clientID("client-id-123")
        .subscribeCallback(new SubscribeCallback() {
          @Override
          public void call(Status status, long sequenceNumber, boolean subscribed) {
          }
        })
        .receiveCallback(new ReceiveCallback() {
          @Override
          public void call(MessageReceived message) {
          }
        })
        .usingFileStorage("/tmp/rocketspeed-storage-file-123");
  }
}
