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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class NoLocalTest {

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
  public void sendMessageReceiveFromQueueWithNoLocal() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection(); ) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 = session.createConsumer(destination, null, true); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumer1).getNoLocal());

            try (PulsarMessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumer1.receiveNoWait());

            try (PulsarConnection connection2 = factory.createConnection()) {
              connection2.start();
              try (PulsarSession session2 = connection2.createSession()) {
                try (PulsarMessageProducer producer = session2.createProducer(destination)) {
                  producer.send(session.createTextMessage("test"));
                }
              }
            }

            // we must be able to receive the message from the second connection
            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("test", textMessage.getText());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromTopicWithNoLocal() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", "false");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection(); ) {
        connection.setClientID("clientId1");
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 = session.createConsumer(destination, null, true); ) {
            assertEquals(
                SubscriptionType.Exclusive,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumer1).getNoLocal());

            try (PulsarMessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumer1.receiveNoWait());

            try (PulsarConnection connection2 = factory.createConnection()) {
              connection2.start();
              try (PulsarSession session2 = connection2.createSession()) {
                try (PulsarMessageProducer producer = session2.createProducer(destination)) {
                  producer.send(session.createTextMessage("test"));
                }
              }
            }

            // we must be able to receive the message from the second connection
            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("test", textMessage.getText());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromExclusiveSubscriptionWithSelector() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", "false");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.setClientID("clientId1");
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 =
              session.createDurableSubscriber(destination, "sub1", null, true); ) {
            assertEquals(
                SubscriptionType.Exclusive,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumer1).getNoLocal());

            try (PulsarMessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumer1.receiveNoWait());

            try (PulsarConnection connection2 = factory.createConnection()) {
              connection2.start();
              try (PulsarSession session2 = connection2.createSession()) {
                try (PulsarMessageProducer producer = session2.createProducer(destination)) {
                  producer.send(session.createTextMessage("test"));
                }
              }
            }

            // we must be able to receive the message from the second connection
            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("test", textMessage.getText());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromSharedSubscriptionWithNoLocal() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", "false");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.setClientID("clientId1");
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 =
              session.createDurableSubscriber(destination, "sub1", null, true); ) {
            assertEquals(
                SubscriptionType.Exclusive,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumer1).getNoLocal());

            try (PulsarMessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumer1.receiveNoWait());

            try (PulsarConnection connection2 = factory.createConnection()) {
              connection2.start();
              try (PulsarSession session2 = connection2.createSession()) {
                try (PulsarMessageProducer producer = session2.createProducer(destination)) {
                  producer.send(session2.createTextMessage("test"));
                }
              }
            }

            // we must be able to receive the message from the second connection
            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("test", textMessage.getText());
          }
        }
      }
    }
  }

  @Test
  public void enableAcknowledgeRejectedMessagesTest() throws Exception {
    acknowledgeRejectedMessagesTest(true);
  }

  @Test
  public void disableAcknowledgeRejectedMessagesTest() throws Exception {
    acknowledgeRejectedMessagesTest(false);
  }

  private void acknowledgeRejectedMessagesTest(boolean acknowledgeRejectedMessages)
      throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", "true");
    properties.put("jms.acknowledgeRejectedMessages", acknowledgeRejectedMessages);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.setClientID("clientId1");
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumerNoLocal =
              session.createConsumer(destination, null, true); ) {
            assertEquals(
                SubscriptionType.Shared, // this is a Queue, so the subscription is always shared
                ((PulsarMessageConsumer) consumerNoLocal).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumerNoLocal).getNoLocal());

            try (PulsarMessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumerNoLocal.receive(3000));
          }
          if (!acknowledgeRejectedMessages) {
            try (PulsarMessageConsumer consumerAllowLocal =
                session.createConsumer(destination, null, false); ) {
              for (int i = 0; i < 10; i++) {
                assertNotNull(consumerAllowLocal.receive());
              }
            }
          } else {
            try (PulsarMessageConsumer consumerAllowLocal =
                session.createConsumer(destination, null, false); ) {
              assertNull(consumerAllowLocal.receive(1000));
            }
          }
        }
      }
    }
  }
}
