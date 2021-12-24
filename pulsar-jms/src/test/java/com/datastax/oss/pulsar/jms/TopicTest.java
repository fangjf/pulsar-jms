/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class TopicTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  public void sendMessageReceiveFromTopic() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 = session.createConsumer(destination);
               PulsarMessageConsumer consumer2 = session.createConsumer(destination); ) {

            assertNotSame(consumer2, consumer1);

            try (PulsarMessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }

            // all of the two consumers receive all of the messages, in order
            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }

            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer2.receive();
              log.info("consumer {} received {}", consumer2, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
            assertNull(consumer2.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void useTopicSubscriberApiWithSharedSubscription() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.clientId", "the-id");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession();
             PulsarSession session2 = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 =
                  session.createDurableSubscriber(destination, "subscription1");
               PulsarMessageConsumer consumer2a =
                  session.createDurableSubscriber(destination, "subscription2")) {

            // it is not possible to create a consumer sharing the same subscription
            try {
              session.createDurableSubscriber(destination, "subscription2");
              fail("should not create two createDurableSubscriber on the same subscription");
            } catch (IllegalStateException err) {
            }

            // it is not possible to create a consumer sharing the same subscription (broker side
            // check)
            try {
              session2.createDurableSubscriber(destination, "subscription2");
              fail("should not create two createDurableSubscriber on the same subscription");
            } catch (JMSException err) {
              assertTrue(err.getCause() instanceof PulsarClientException.ConsumerBusyException);
            }

            try (PulsarMessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }

            // consumer1 receives all messages, in order
            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }

            // let consumer2a receive the first half of the message
            for (int i = 0; i < 5; i++) {
              TextMessage msg = (TextMessage) consumer2a.receive();
              log.info("consumer {} received {}", consumer2a, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }

            // closing consumer 2a
            consumer2a.close();

            // let consumer2b receive the second half of the message
            try (PulsarMessageConsumer consumer2b =
                session.createDurableSubscriber(destination, "subscription2")) {
              for (int i = 5; i < 10; i++) {
                TextMessage msg = (TextMessage) consumer2b.receive();
                log.info("consumer {} received {}", consumer2b, msg.getText());
                assertEquals("foo-" + i, msg.getText());
              }
              assertNull(consumer2b.receiveNoWait());
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void simpleDurableConsumerTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection1 = factory.createConnection();
           PulsarConnection connection2 = factory.createConnection()) {
        connection2.setClientID("testclient");
        connection1.start();
        connection2.start();
        try (PulsarSession session1 = connection1.createSession();
             PulsarSession session2 = connection2.createSession()) {
          String topicName = "persistent://public/default/test-" + UUID.randomUUID();
          Topic topic = session1.createTopic(topicName);
          String durableSubscriptionName = "simpleDurableConsumerTest";

          PulsarMessageConsumer consumer2 = session2.createDurableSubscriber(topic, durableSubscriptionName);

          PulsarMessageProducer producer = session1.createProducer(topic);

          TextMessage messageSent = session1.createTextMessage("just a test");
          messageSent.setStringProperty("COM_SUN_JMS_TESTNAME", durableSubscriptionName);
          producer.send(messageSent);
          TextMessage messageReceived = (TextMessage) consumer2.receive(5000);

          // Check to see if correct message received
          assertEquals(messageReceived.getText(), messageSent.getText());
        }
      }
    }
  }

  @Test
  public void testUseKeySharedSubscriptionTypeforTopicConsumer() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.useExclusiveSubscriptionsForSimpleConsumers", "false");
    properties.put("jms.topicSharedSubscriptionType", SubscriptionType.Key_Shared);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession();
             PulsarSession session2 = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertEquals(
                SubscriptionType.Key_Shared,
                consumer1.getSubscriptionType());

            try (PulsarMessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }

            // consumer1 receives all messages, in order
            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }
          }
        }
      }
    }
  }
}
