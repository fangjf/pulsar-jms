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
package com.datastax.oss.pulsar.jms.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.pulsar.jms.PulsarConnection;
import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.datastax.oss.pulsar.jms.PulsarSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DockerTest {

  @TempDir public Path temporaryDir;

  @Test
  public void testPulsar272() throws Exception {
    test("apachepulsar/pulsar:2.7.2", false);
  }

  @Test
  public void testPulsar281() throws Exception {
    test("apachepulsar/pulsar:2.8.1", false);
  }

  @Test
  public void testPulsar281Transactions() throws Exception {
    test("apachepulsar/pulsar:2.8.1", true);
  }

  private void test(String image, boolean transactions) throws Exception {
    try (PulsarContainer pulsarContainer = new PulsarContainer(image, transactions); ) {
      pulsarContainer.start();
      Map<String, Object> properties = new HashMap<>();
      properties.put("brokerServiceUrl", pulsarContainer.getPulsarBrokerUrl());
      properties.put("webServiceUrl", pulsarContainer.getHttpServiceUrl());
      properties.put("enableTransaction", transactions);
      try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
           PulsarConnection connection1 = factory.createConnection();
           PulsarConnection connection2 = factory.createConnection()) {
        connection1.start();
        connection2.start();
        try (PulsarSession session1 = connection1.createSession(
                transactions ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
             PulsarSession session2 = connection2.createSession(
                     transactions ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE)) {
          Destination queue = session1.createQueue("test");
          MessageProducer producer = session1.createProducer(queue);
          producer.send(session1.createTextMessage("foo"));
        if (transactions) {
          session1.commit();
        }
        TextMessage message = (TextMessage) session2.createConsumer(queue).receive();
        assertEquals("foo", message.getText());
        if (transactions) {
          session2.commit();
        }
      }
      }
    }
  }
}
