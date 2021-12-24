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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 1, unit = TimeUnit.MINUTES)
public class SimpleTest {

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
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        try (PulsarSession session = connection.createSession(); ) {
          Destination destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (PulsarMessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo");
            producer.send(textMsg);

            StreamMessage streamMessage = session.createStreamMessage();
            streamMessage.writeBytes("foo".getBytes(StandardCharsets.UTF_8));
            producer.send(streamMessage);

            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeInt(234);
            producer.send(bytesMessage);

            Message headerOnly = session.createMessage();
            headerOnly.setBooleanProperty("myproperty", true);
            producer.send(headerOnly);

            ObjectMessage objectMessage = session.createObjectMessage("test");
            producer.send(objectMessage);

            MapMessage mapMessage = session.createMapMessage();
            mapMessage.setBoolean("p1", true);
            producer.send(mapMessage);
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceive() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Destination destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer = session.createConsumer(destination); ) {

            try (PulsarMessageProducer producer = session.createProducer(destination); ) {
              TextMessage textMsg = session.createTextMessage("foo");
              producer.send(textMsg);
              ObjectMessage objectMsg = session.createObjectMessage("bar");
              producer.send(objectMsg);
              BytesMessage bytesMsg = session.createBytesMessage();
              bytesMsg.writeInt(1234);
              producer.send(bytesMsg);
              StreamMessage streamMessage = session.createStreamMessage();
              streamMessage.writeLong(1234);
              producer.send(streamMessage);
              MapMessage mapMessage = session.createMapMessage();
              mapMessage.setBoolean("foo", true);
              mapMessage.setString("bar", "test");
              producer.send(mapMessage);
              Message simpleMessage = session.createMessage();
              simpleMessage.setByteProperty("a", (byte) 1);
              simpleMessage.setLongProperty("b", 123232323233L);
              simpleMessage.setIntProperty("c", 1232323);
              simpleMessage.setStringProperty("d", "ttt");
              simpleMessage.setBooleanProperty("e", true);
              simpleMessage.setFloatProperty("f", 1.3f);
              simpleMessage.setDoubleProperty("g", 1.9d);
              simpleMessage.setShortProperty("h", (short) 89);
              simpleMessage.setJMSType("mytype");
              simpleMessage.setJMSCorrelationID("correlationid");
              simpleMessage.setObjectProperty("i", 1.3d);
              producer.send(simpleMessage, DeliveryMode.NON_PERSISTENT, 2, 0);

              Message simpleMessage2 = session.createMessage();
              simpleMessage2.setJMSCorrelationIDAsBytes(new byte[] {1, 2, 3});
              producer.send(simpleMessage2, DeliveryMode.PERSISTENT, 3, 0);
            }

            TextMessage msg = (TextMessage) consumer.receive();
            assertEquals("foo", msg.getText());
            ObjectMessage msg2 = (ObjectMessage) consumer.receive();
            assertEquals("bar", msg2.getObject());
            BytesMessage msg3 = (BytesMessage) consumer.receive();
            assertEquals(1234, msg3.readInt());
            StreamMessage msg4 = (StreamMessage) consumer.receive();
            assertEquals(1234l, msg4.readLong());
            MapMessage msg5 = (MapMessage) consumer.receive();
            assertEquals(true, msg5.getBoolean("foo"));
            assertEquals("test", msg5.getString("bar"));
            Message msg6 = consumer.receive();

            assertEquals((byte) 1, msg6.getByteProperty("a"));
            assertEquals(123232323233L, msg6.getLongProperty("b"));
            assertEquals(1232323, msg6.getIntProperty("c"));
            assertEquals("ttt", msg6.getStringProperty("d"));
            assertEquals(true, msg6.getBooleanProperty("e"));
            assertEquals(1.3f, msg6.getFloatProperty("f"), 0);
            assertEquals(1.9d, msg6.getDoubleProperty("g"), 0);
            assertEquals(89, msg6.getShortProperty("h"));
            assertEquals(2, msg6.getJMSPriority());
            assertEquals("mytype", msg6.getJMSType());
            assertEquals(DeliveryMode.NON_PERSISTENT, msg6.getJMSDeliveryMode());
            assertEquals("correlationid", msg6.getJMSCorrelationID());
            assertArrayEquals(
                "correlationid".getBytes(StandardCharsets.UTF_8),
                msg6.getJMSCorrelationIDAsBytes());
            // we are serializing Object properties as strings
            assertEquals(1.3d, msg6.getObjectProperty("i"));

            Message msg7 = consumer.receive();

            assertEquals(DeliveryMode.PERSISTENT, msg7.getJMSDeliveryMode());
            assertArrayEquals(new byte[] {1, 2, 3}, msg7.getJMSCorrelationIDAsBytes());
          }
        }
      }
    }
  }

  @Test
  public void systemNameSpaceTest() throws Exception {

    String simpleName = "test-" + UUID.randomUUID().toString();
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.systemNamespace", "pulsar/system");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          PulsarDestination destination =
              session.createQueue("persistent://pulsar/system/" + simpleName);

          PulsarDestination destinationWithSimpleName = session.createQueue(simpleName);

          assertEquals(destination.getName(), factory.applySystemNamespace(destination.getName()));

          try (PulsarMessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo1");
            producer.send(textMsg);
          }

          try (PulsarMessageProducer producer = session.createProducer(destinationWithSimpleName); ) {
            TextMessage textMsg = session.createTextMessage("foo2");
            producer.send(textMsg);
          }

          try (PulsarMessageConsumer consumer = session.createConsumer(destination); ) {
            TextMessage msg1 = (TextMessage) consumer.receive();
            assertEquals("foo1", msg1.getText());
            TextMessage msg2 = (TextMessage) consumer.receive();
            assertEquals("foo2", msg2.getText());
          }
        }
      }
    }
  }
}
