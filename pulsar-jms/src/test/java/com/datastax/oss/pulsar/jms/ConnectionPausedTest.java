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
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class ConnectionPausedTest {

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
  @Timeout(60)
  public void pausedConnectionTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.setClientID("a");
        // DO NOT START THE CONNECTION
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          TextMessage textMsg = session.createTextMessage("foo");
          try (PulsarMessageProducer producer = session.createProducer(destination); ) {
            try (PulsarMessageConsumer consumer =
                session.createDurableSubscriber(destination, "sub1")) {

              // send many messages
              producer.send(textMsg);
              producer.send(textMsg);
              producer.send(textMsg);
              producer.send(textMsg);

              ScheduledExecutorService executeLater = Executors.newSingleThreadScheduledExecutor();
              try {

                executeLater.schedule(
                    () -> Utils.noException(() -> connection.start()), 5, TimeUnit.SECONDS);

                // block until the connection is started
                // the connection will be started in 5 seconds and the test won't be stuck
                TextMessage message = (TextMessage)consumer.receive();
                assertEquals("foo", message.getText());

                connection.stop();

                // if the connection is stopped and there is a timeout we must return null
                assertNull(consumer.receive(2000));
                assertNull(consumer.receiveNoWait());

                connection.start();

                // now we are able to receive all of the remaining messages
                message = (TextMessage) consumer.receive(2000);
                assertEquals("foo", message.getText());
                message = (TextMessage) consumer.receiveNoWait();
                assertEquals("foo", message.getText());
                message = (TextMessage) consumer.receive();
                assertEquals("foo", message.getText());

              } finally {
                executeLater.shutdown();
              }
            }
          }
        }
      }
    }
  }

  @Test
  @Timeout(60)
  public void stopConnectionMustWaitForPendingReceive() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    CountDownLatch beforeReceive = new CountDownLatch(1);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.setClientID("a");
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          TextMessage textMsg = session.createTextMessage("foo");
          try (PulsarMessageProducer producer = session.createProducer(destination); ) {

            CompletableFuture<Message> consumerResult = new CompletableFuture<>();
            Thread consumerThread =
                new Thread(
                    () -> {
                      try (PulsarSession consumerSession = connection.createSession();
                           PulsarMessageConsumer consumer =
                              consumerSession.createDurableSubscriber(destination, "sub1")) {
                        // no message in the topic, so this consumer will hang
                        beforeReceive.countDown();
                        log.info("receiving...");
                        consumerResult.complete(consumer.receive());
                      } catch (Throwable err) {
                        consumerResult.completeExceptionally(err);
                      }
                    },
                    "consumer-test-thread");

            consumerThread.start();

            // wait for the consumer to block on "receive"
            beforeReceive.await();
            // wait to enter "receive" method and blocks
            Thread.sleep(1000);

            log.info("Consumer thread status {}", consumerThread);
            Stream.of(consumerThread.getStackTrace()).forEach(t -> System.err.println(t));
            assertEquals(Thread.State.TIMED_WAITING, consumerThread.getState());

            ScheduledExecutorService executeLater = Executors.newSingleThreadScheduledExecutor();
            try {

              executeLater.schedule(
                  () -> Utils.noException(() -> producer.send(textMsg)), 5, TimeUnit.SECONDS);

              // as we have one consumer that is blocked Connection#stop must block
              connection.stop();

              // ensure that the consumer received the message
              TextMessage message = (TextMessage)consumerResult.get();
              assertEquals("foo", message.getText());

            } finally {
              executeLater.shutdown();
            }
          }
        }
      }
    }
  }
}
