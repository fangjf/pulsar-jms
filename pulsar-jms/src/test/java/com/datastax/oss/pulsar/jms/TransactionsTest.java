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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
@Disabled
public class TransactionsTest {

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
  public void sendMessageTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();

        try (PulsarSession consumerSession = connection.createSession(); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (PulsarMessageConsumer consumer = consumerSession.createConsumer(destination)) {

            try (PulsarSession transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

              try (PulsarMessageProducer producer = transaction.createProducer(destination); ) {
                TextMessage textMsg = transaction.createTextMessage("foo");
                producer.send(textMsg);
                producer.send(textMsg);
              }

              // message is not "visible" as transaction is not committed
              assertNull(consumer.receive(1000));

              transaction.commit();

              // message is now visible to consumers
              assertNotNull(consumer.receive());
              assertNotNull(consumer.receive());
            }
          }
        }
      }
    }
  }

  @Test
  public void autoRollbackTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();

        try (PulsarSession consumerSession = connection.createSession(); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (PulsarMessageConsumer consumer = consumerSession.createConsumer(destination)) {

            try (PulsarSession transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

              try (PulsarMessageProducer producer = transaction.createProducer(destination); ) {
                TextMessage textMsg = transaction.createTextMessage("foo");
                producer.send(textMsg);
              }

              // message is not "visible" as transaction is not committed
              assertNull(consumer.receive(1000));

              // session closed -> auto rollback
            }
            // message is lost
            assertNull(consumer.receive(1000));
          }
        }
      }
    }
  }

  @Test
  public void rollbackProduceTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();

        try (PulsarSession consumerSession = connection.createSession(); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (PulsarMessageConsumer consumer = consumerSession.createConsumer(destination)) {

            try (PulsarSession transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

              try (PulsarMessageProducer producer = transaction.createProducer(destination); ) {
                TextMessage textMsg = transaction.createTextMessage("foo");
                producer.send(textMsg);
              }

              // message is not "visible" as transaction is not committed
              assertNull(consumer.receive(1000));

              transaction.rollback();

              // message is lost
              assertNull(consumer.receive(1000));
            }
          }
        }
      }
    }
  }

  @Test
  public void consumeTransactionTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();

        try (PulsarSession producerSession = connection.createSession(); ) {
          Destination destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarSession transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (PulsarMessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (PulsarMessageProducer producer = producerSession.createProducer(destination); ) {
                TextMessage textMsg = producerSession.createTextMessage("foo");
                producer.send(textMsg);
              }

              TextMessage receive = (TextMessage)consumer.receive();
              assertEquals("foo", receive.getText());
            }

            transaction.commit();

            // message has been committed by the transacted session
            try (PulsarMessageConsumer consumer = producerSession.createConsumer(destination); ) {
              assertNull(consumer.receive(1000));
            }
          }
        }
      }
    }
  }

  @Test
  public void multiCommitTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();

        try (PulsarSession producerSession = connection.createSession(); ) {
          Destination destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarSession transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (PulsarMessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (PulsarMessageProducer producer = producerSession.createProducer(destination); ) {
                producer.send(producerSession.createTextMessage("foo0"));
                producer.send(producerSession.createTextMessage("foo1"));
              }

              TextMessage receive = (TextMessage) consumer.receive();
              assertEquals("foo0", receive.getText());
              transaction.commit();

              receive = (TextMessage) consumer.receive();
              assertEquals("foo1", receive.getText());
              transaction.commit();
            }

            // messages have been committed by the transacted session
            try (PulsarMessageConsumer consumer = producerSession.createConsumer(destination); ) {
              assertNull(consumer.receive(1000));
            }
          }
        }
      }
    }
  }

  @Test
  public void consumeRollbackTransactionTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();

        try (PulsarSession producerSession = connection.createSession(); ) {
          Destination destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarSession transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (PulsarMessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (PulsarMessageProducer producer = producerSession.createProducer(destination); ) {
                TextMessage textMsg = producerSession.createTextMessage("foo");
                producer.send(textMsg);
              }

              TextMessage receive = (TextMessage) consumer.receive();
              assertEquals("foo", receive.getText());
            }

            transaction.rollback();

            // the consumer rolledback the transaction, now we can receive the message from
            // another client
            try (PulsarMessageConsumer consumer = producerSession.createConsumer(destination); ) {
              assertNotNull(consumer.receive());
            }
          }
        }
      }
    }
  }

  @Test
  public void consumeAutoRollbackTransactionTestWithQueueBrowser() throws Exception {

    int numMessages = 10;
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection();
           PulsarConnection connection2 = factory.createConnection()) {
        connection.start();

        try (PulsarSession producerSession = connection.createSession(); ) {
          Queue destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarSession transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (PulsarMessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (PulsarMessageProducer producer = producerSession.createProducer(destination); ) {
                for (int i = 0; i < numMessages; i++) {
                  TextMessage textMsg = producerSession.createTextMessage("foo" + i);
                  producer.send(textMsg);
                }
              }

              try (PulsarQueueBrowser counter = producerSession.createBrowser(destination)) {
                int count = 0;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }

              // transactional consumer, receives but it does not commit
              TextMessage receive = (TextMessage)consumer.receive();
              assertEquals("foo0", receive.getText());

              // the QueueBrowser still sees the message
              try (PulsarQueueBrowser counter = producerSession.createBrowser(destination)) {
                int count = 0;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }
            }

            connection.close();

            connection2.start();

            try (PulsarSession secondSession = connection2.createSession();
                 PulsarMessageConsumer consumer = secondSession.createConsumer(destination); ) {
              assertNotNull(consumer.receive());

              // it looks like peekMessage is not following the subscription in realtime
              Thread.sleep(2000);

              // the QueueBrowser does not see the consumed message anymore
              try (PulsarQueueBrowser counter = secondSession.createBrowser(destination)) {
                // skip first message
                int count = 1;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void rollbackReceivedMessages() throws Exception {

    int numMessages = 10;
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection();
           PulsarConnection connection2 = factory.createConnection()) {
        connection.start();

        try (PulsarSession producerSession = connection.createSession(); ) {
          Queue destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarSession transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (PulsarMessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (PulsarMessageProducer producer = producerSession.createProducer(destination); ) {
                for (int i = 0; i < numMessages; i++) {
                  TextMessage textMsg = producerSession.createTextMessage("foo" + i);
                  producer.send(textMsg);
                }
              }

              TextMessage receive = (TextMessage) consumer.receive();
              log.info("receive and commit {}", receive.getText());
              assertEquals(numMessages, countMessages(producerSession, destination));
              transaction.commit();
              assertEquals(numMessages - 1, countMessages(producerSession, destination));

              receive = (TextMessage) consumer.receive();
              log.info("receive and rollback {}", receive.getText());
              transaction.rollback();
              assertEquals(numMessages - 1, countMessages(producerSession, destination));

              receive = (TextMessage) consumer.receive();

              log.info("receive {}", receive.getText());
              assertEquals(numMessages - 1, countMessages(producerSession, destination));
              log.info("commit final");
              transaction.commit();
              assertEquals(numMessages - 2, countMessages(producerSession, destination));
            }
          }
        }
      }
    }
  }

  private static int countMessages(PulsarSession producerSession, Queue destination) throws JMSException {
    int count = 0;
    try (PulsarQueueBrowser counter = producerSession.createBrowser(destination)) {
      for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
        TextMessage msg = (TextMessage) e.nextElement();
        log.info("count {} msg {}", count, msg.getText());
        count++;
      }
    }
    return count;
  }

  @Test
  public void consumeRollbackTransactionTestWithQueueBrowser() throws Exception {

    int numMessages = 10;
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection();
           PulsarConnection connection2 = factory.createConnection()) {
        connection.start();

        try (PulsarSession producerSession = connection.createSession(); ) {
          Queue destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarSession transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (PulsarMessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (PulsarMessageProducer producer = producerSession.createProducer(destination); ) {
                for (int i = 0; i < numMessages; i++) {
                  TextMessage textMsg = producerSession.createTextMessage("foo" + i);
                  producer.send(textMsg);
                }
              }

              try (PulsarQueueBrowser counter = producerSession.createBrowser(destination)) {
                int count = 0;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }

              // transactional consumer, receives but it does not commit
              TextMessage receive = (TextMessage)consumer.receive();
              assertEquals("foo0", receive.getText());

              // the QueueBrowser still sees the message
              try (PulsarQueueBrowser counter = producerSession.createBrowser(destination)) {
                int count = 0;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }
            }

            transaction.rollback();

            connection2.start();

            try (PulsarSession secondSession = connection2.createSession();
                 PulsarMessageConsumer consumer = secondSession.createConsumer(destination); ) {
              assertNotNull(consumer.receive());

              // it looks like peekMessage is not following the subscription in realtime
              Thread.sleep(2000);

              // the QueueBrowser does not see the consumed message anymore
              try (PulsarQueueBrowser counter = secondSession.createBrowser(destination)) {
                // skip first message
                int count = 1;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }
            }
          }
        }
      }
    }
  }
}
