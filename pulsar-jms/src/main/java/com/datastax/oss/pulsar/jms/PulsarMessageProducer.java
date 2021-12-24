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

import com.datastax.oss.pulsar.jms.messages.PulsarBytesMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarMapMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarObjectMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarSimpleMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarStreamMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarTextMessage;
import java.util.Enumeration;
import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;

@Slf4j
class PulsarMessageProducer implements MessageProducer, TopicPublisher, QueueSender, AutoCloseable {
  private final PulsarSession session;
  private final PulsarDestination defaultDestination;
  private final boolean jms20;

  public PulsarMessageProducer(PulsarSession session, Destination defaultDestination)
      throws JMSException {
    this.jms20 = session.isJms20();
    session.checkNotClosed();
    this.session = session;
    try {
      this.defaultDestination = (PulsarDestination) defaultDestination;
    } catch (ClassCastException err) {
      throw new InvalidDestinationException(
          "Invalid destination type " + defaultDestination.getClass());
    }
  }

  private boolean closed;
  private boolean disableMessageId;
  private boolean disableMessageTimestamp;
  private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
  private int priority = Message.DEFAULT_PRIORITY;
  private long defaultTimeToLive = Message.DEFAULT_TIME_TO_LIVE;

  /**
   * Specify whether message IDs may be disabled.
   *
   * <p>Since message IDs take some effort to create and increase a message's size, some JMS
   * providers may be able to optimise message overhead if they are given a hint that the message ID
   * is not used by an application. By calling this method, a JMS application enables this potential
   * optimisation for all messages sent using this {@code MessageProducer}. If the JMS provider
   * accepts this hint, these messages must have the message ID set to null; if the provider ignores
   * the hint, the message ID must be set to its normal unique value.
   *
   * <p>Message IDs are enabled by default.
   *
   * @param value indicates if message IDs may be disabled
   * @throws JMSException if the JMS provider fails to set message ID to disabled due to some
   *     internal error.
   */
  @Override
  public void setDisableMessageID(boolean value) throws JMSException {
    checkNotClosed();
    this.disableMessageId = value;
  }

  /**
   * Gets an indication of whether message IDs are disabled.
   *
   * @return an indication of whether message IDs are disabled
   * @throws JMSException if the JMS provider fails to determine if message IDs are disabled due to
   *     some internal error.
   */
  @Override
  public boolean getDisableMessageID() throws JMSException {
    checkNotClosed();
    return disableMessageId;
  }

  /**
   * Specify whether message timestamps may be disabled.
   *
   * <p>Since timestamps take some effort to create and increase a message's size, some JMS
   * providers may be able to optimise message overhead if they are given a hint that the timestamp
   * is not used by an application. By calling this method, a JMS application enables this potential
   * optimisation for all messages sent using this {@code MessageProducer}. If the JMS provider
   * accepts this hint, these messages must have the timestamp set to zero; if the provider ignores
   * the hint, the timestamp must be set to its normal value.
   *
   * <p>Message timestamps are enabled by default.
   *
   * @param value indicates whether message timestamps may be disabled
   * @throws JMSException if the JMS provider fails to set timestamps to disabled due to some
   *     internal error.
   */
  @Override
  public void setDisableMessageTimestamp(boolean value) throws JMSException {
    checkNotClosed();
    this.disableMessageTimestamp = value;
  }

  /**
   * Gets an indication of whether message timestamps are disabled.
   *
   * @return an indication of whether message timestamps are disabled
   * @throws JMSException if the JMS provider fails to determine if timestamps are disabled due to
   *     some internal error.
   */
  @Override
  public boolean getDisableMessageTimestamp() throws JMSException {
    checkNotClosed();
    return disableMessageTimestamp;
  }

  /**
   * Sets the producer's default delivery mode.
   *
   * <p>Delivery mode is set to {@code PERSISTENT} by default.
   *
   * @param deliveryMode the message delivery mode for this message producer; legal values are
   *     {@code DeliveryMode.NON_PERSISTENT} and {@code DeliveryMode.PERSISTENT}
   * @throws JMSException if the JMS provider fails to set the delivery mode due to some internal
   *     error.
   * @see MessageProducer#getDeliveryMode
   * @see DeliveryMode#NON_PERSISTENT
   * @see DeliveryMode#PERSISTENT
   * @see Message#DEFAULT_DELIVERY_MODE
   */
  @Override
  public void setDeliveryMode(int deliveryMode) throws JMSException {
    checkNotClosed();
    validateDeliveryMode(deliveryMode);
    this.deliveryMode = deliveryMode;
  }

  private static void validateDeliveryMode(int deliveryMode) throws JMSException {
    switch (deliveryMode) {
      case DeliveryMode.NON_PERSISTENT:
      case DeliveryMode.PERSISTENT:
        break;
      default:
        throw new JMSException("Invalid deliveryMode " + deliveryMode);
    }
  }

  /**
   * Gets the producer's default delivery mode.
   *
   * @return the message delivery mode for this message producer
   * @throws JMSException if the JMS provider fails to get the delivery mode due to some internal
   *     error.
   * @see MessageProducer#setDeliveryMode
   */
  @Override
  public int getDeliveryMode() throws JMSException {
    checkNotClosed();
    return deliveryMode;
  }

  private void checkNotClosed() throws JMSException {
    session.checkNotClosed();
    if (closed) {
      throw new IllegalStateException("this producer is closed");
    }
  }

  /**
   * Sets the producer's default priority.
   *
   * <p>The JMS API defines ten levels of priority value, with 0 as the lowest priority and 9 as the
   * highest. Clients should consider priorities 0-4 as gradations of normal priority and priorities
   * 5-9 as gradations of expedited priority. Priority is set to 4 by default.
   *
   * @param defaultPriority the message priority for this message producer; must be a value between
   *     0 and 9
   * @throws JMSException if the JMS provider fails to set the priority due to some internal error.
   * @see MessageProducer#getPriority
   * @see Message#DEFAULT_PRIORITY
   */
  @Override
  public void setPriority(int defaultPriority) throws JMSException {
    checkNotClosed();
    validatePriority(defaultPriority);
    this.priority = defaultPriority;
  }

  private void validatePriority(int defaultPriority) throws JMSException {
    if (defaultPriority < 0 || defaultPriority > 10) {
      throw new JMSException("invalid priority " + defaultPriority);
    }
  }

  /**
   * Gets the producer's default priority.
   *
   * @return the message priority for this message producer
   * @throws JMSException if the JMS provider fails to get the priority due to some internal error.
   * @see MessageProducer#setPriority
   */
  @Override
  public int getPriority() throws JMSException {
    checkNotClosed();
    return priority;
  }

  /**
   * Sets the default length of time in milliseconds from its dispatch time that a produced message
   * should be retained by the message system.
   *
   * <p>Time to live is set to zero by default.
   *
   * @param timeToLive the message time to live in milliseconds; zero is unlimited
   * @throws JMSException if the JMS provider fails to set the time to live due to some internal
   *     error.
   * @see MessageProducer#getTimeToLive
   * @see Message#DEFAULT_TIME_TO_LIVE
   */
  @Override
  public void setTimeToLive(long timeToLive) throws JMSException {
    checkNotClosed();
    this.defaultTimeToLive = timeToLive;
  }

  /**
   * Gets the default length of time in milliseconds from its dispatch time that a produced message
   * should be retained by the message system.
   *
   * @return the message time to live in milliseconds; zero is unlimited
   * @throws JMSException if the JMS provider fails to get the time to live due to some internal
   *     error.
   * @see MessageProducer#setTimeToLive
   */
  @Override
  public long getTimeToLive() throws JMSException {
    checkNotClosed();
    return defaultTimeToLive;
  }

  /**
   * Gets the destination associated with this {@code MessageProducer}.
   *
   * @return this producer's {@code Destination}
   * @throws JMSException if the JMS provider fails to get the destination for this {@code
   *     MessageProducer} due to some internal error.
   * @since JMS 1.1
   */
  @Override
  public Destination getDestination() throws JMSException {
    checkNotClosed();
    return defaultDestination;
  }

  /**
   * Closes the message producer.
   *
   * <p>Since a provider may allocate some resources on behalf of a {@code MessageProducer} outside
   * the Java virtual machine, clients should close them when they are not needed. Relying on
   * garbage collection to eventually reclaim these resources may not be timely enough.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <tt>MessageProducer</tt> have been completed and any <tt>CompletionListener</tt> callbacks have
   * returned. Incomplete sends should be allowed to complete normally unless an error occurs.
   *
   * <p>A <tt>CompletionListener</tt> callback method must not call <tt>close</tt> on its own
   * <tt>MessageProducer</tt>. Doing so will cause an <tt>IllegalStateException</tt> to be thrown.
   *
   * @throws IllegalStateException this method has been called by a <tt>CompletionListener</tt>
   *     callback method on its own <tt>MessageProducer</tt>
   * @throws JMSException if the JMS provider fails to close the producer due to some internal
   *     error.
   */
  @Override
  public void close() throws JMSException {
    Utils.checkNotOnMessageProducer(session, this);
    closed = true;
  }

  /**
   * Sends a message using the {@code MessageProducer}'s default delivery mode, priority, and time
   * to live.
   *
   * @param message the message to send
   * @throws JMSException if the JMS provider fails to send the message due to some internal error.
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with a {@code MessageProducer}
   *     with an invalid destination.
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that did not specify a destination at creation time.
   * @see Session#createProducer
   * @since JMS 1.1
   */
  @Override
  public void send(Message message) throws JMSException {
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    validateMessageSend(
        message, defaultDestination, true, Message.DEFAULT_TIME_TO_LIVE, deliveryMode, priority);
    sendMessage(defaultDestination, message);
  }

  /**
   * Sends a message, specifying delivery mode, priority, and time to live.
   *
   * @param message the message to send
   * @param deliveryMode the delivery mode to use
   * @param priority the priority for this message
   * @param timeToLive the message's lifetime (in milliseconds)
   * @throws JMSException if the JMS provider fails to send the message due to some internal error.
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with a {@code MessageProducer}
   *     with an invalid destination.
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that did not specify a destination at creation time.
   * @see Session#createProducer
   * @since JMS 1.1
   */
  @Override
  public void send(Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException {
    validateMessageSend(message, defaultDestination, true, timeToLive, deliveryMode, priority);
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    applyTimeToLive(message, timeToLive);
    sendMessage(defaultDestination, message);
  }

  /**
   * Sends a message to a destination for an unidentified message producer using the {@code
   * MessageProducer}'s default delivery mode, priority, and time to live.
   *
   * <p>Typically, a message producer is assigned a destination at creation time; however, the JMS
   * API also supports unidentified message producers, which require that the destination be
   * supplied every time a message is sent.
   *
   * @param destination the destination to send this message to
   * @param message the message to send
   * @throws JMSException if the JMS provider fails to send the message due to some internal error.
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with an invalid destination.
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that specified a destination at creation time.
   * @see Session#createProducer
   * @since JMS 1.1
   */
  @Override
  public void send(Destination destination, Message message) throws JMSException {
    checkNoDefaultDestinationSet();
    validateMessageSend(
        message, destination, false, Message.DEFAULT_TIME_TO_LIVE, deliveryMode, priority);
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    sendMessage(destination, message);
  }

  /**
   * Sends a message to a destination for an unidentified message producer, specifying delivery
   * mode, priority and time to live.
   *
   * <p>Typically, a message producer is assigned a destination at creation time; however, the JMS
   * API also supports unidentified message producers, which require that the destination be
   * supplied every time a message is sent.
   *
   * @param destination the destination to send this message to
   * @param message the message to send
   * @param deliveryMode the delivery mode to use
   * @param priority the priority for this message
   * @param timeToLive the message's lifetime (in milliseconds)
   * @throws JMSException if the JMS provider fails to send the message due to some internal error.
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with an invalid destination.
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that specified a destination at creation time.
   * @see Session#createProducer
   * @since JMS 1.1
   */
  @Override
  public void send(
      Destination destination, Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException {
    checkNoDefaultDestinationSet();
    validateMessageSend(message, destination, false, timeToLive, deliveryMode, priority);
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    applyTimeToLive(message, timeToLive);
    sendMessage(destination, message);
  }

  private void checkNoDefaultDestinationSet() {
    if (defaultDestination != null) {
      throw new UnsupportedOperationException(
          "you cannot use this producer with another destination");
    }
  }

  private void validateMessageSend(
      Message message,
      Destination destination,
      boolean isDefaultDestination,
      long timeToLive,
      int deliveryMode,
      int priority)
      throws JMSException {
    checkNotClosed();
    if (message == null) {
      throw new MessageFormatException("Invalid null message");
    }
    if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT) {
      throw new JMSException("Invalid deliveryMode " + deliveryMode);
    }
    validatePriority(priority);
    if (destination == null) {
      if (isDefaultDestination) {
        throw new UnsupportedOperationException("please set a destination");
      } else {
        throw new InvalidDestinationException("destination is null");
      }
    }
    if (timeToLive > 0 && !session.getFactory().isEnableClientSideEmulation()) {
      throw new JMSException(
          "timeToLive not enabled, please set jms.enableClientSideEmulation=true");
    }
  }

  private PulsarMessage prepareMessageForSend(Message message) throws JMSException {
    if (message == null) {
      throw new IllegalArgumentException("Cannot send a null message");
    }
    PulsarMessage res;
    if (!(message instanceof PulsarMessage)) {
      if (message instanceof TextMessage) {
        res = new PulsarTextMessage(((TextMessage) message).getText());
      } else if (message instanceof BytesMessage) {
        BytesMessage sm = (BytesMessage) message;
        sm.reset();
        byte[] buffer = new byte[(int) sm.getBodyLength()];
        sm.readBytes(buffer);
        PulsarBytesMessage dest = new PulsarBytesMessage(buffer);
        res = dest;
      } else if (message instanceof MapMessage) {
        MapMessage sm = (MapMessage) message;
        PulsarMapMessage dest = new PulsarMapMessage();
        for (Enumeration en = sm.getMapNames(); en.hasMoreElements(); ) {
          String name = (String) en.nextElement();
          dest.setObject(name, sm.getObject(name));
        }
        res = dest;
      } else if (message instanceof ObjectMessage) {
        res = new PulsarObjectMessage(((ObjectMessage) message).getObject());
      } else if (message instanceof StreamMessage) {
        StreamMessage sm = (StreamMessage) message;
        sm.reset();
        PulsarStreamMessage dest = new PulsarStreamMessage();
        while (true) {
          try {
            Object object = sm.readObject();
            dest.writeObject(object);
          } catch (MessageEOFException end) {
            break;
          }
        }
        res = dest;
      } else {
        res = new PulsarSimpleMessage();
      }
      res.setWritable(true);
      for (Enumeration en = message.getPropertyNames(); en.hasMoreElements(); ) {
        String name = (String) en.nextElement();
        res.setObjectProperty(name, message.getObjectProperty(name));
      }
      res.setJMSCorrelationIDAsBytes(message.getJMSCorrelationIDAsBytes());
      res.setJMSDeliveryMode(message.getJMSDeliveryMode());
      res.setJMSPriority(message.getJMSPriority());
      res.setJMSDestination(message.getJMSDestination());

      // DO NOT COPY THESE VALUES
      // res.setJMSMessageID(message.getJMSMessageID());
      // res.setJMSTimestamp(message.getJMSTimestamp());
      // res.setJMSExpiration(message.getJMSExpiration());
    } else {
      res = (PulsarMessage) message;
    }
    res.setWritable(true);
    res.setStringProperty("JMSConnectionID", session.getConnection().getConnectionId());
    return res;
  }

  private void applyTimeToLive(Message message, long timeToLive) throws JMSException {
    if (timeToLive > 0) {
      long time = System.currentTimeMillis() + timeToLive;
      message.setLongProperty("JMSExpiration", System.currentTimeMillis() + timeToLive);
      message.setJMSExpiration(time);
    }
  }

  private void sendMessage(Destination defaultDestination, Message message) throws JMSException {
    if (message == null) {
      throw new MessageFormatException("null message");
    }
    Producer<byte[]> producer =
        session.getFactory().getProducerForDestination(defaultDestination, session.getTransacted());
    message.setJMSDestination(defaultDestination);
    PulsarMessage pulsarMessage = prepareMessageForSend(message);
    final TypedMessageBuilder<byte[]> typedMessageBuilder;
    if (session.getTransacted()) {
      typedMessageBuilder = producer.newMessage(session.getTransaction());
    } else {
      typedMessageBuilder = producer.newMessage();
    }
    pulsarMessage.send(typedMessageBuilder, disableMessageTimestamp, session);
    if (message != pulsarMessage) {
      applyBackMessageProperties(message, pulsarMessage);
    }
  }

  private void applyBackMessageProperties(Message message, PulsarMessage pulsarMessage) {
    Utils.runtimeException(
        () -> {
          message.setJMSTimestamp(pulsarMessage.getJMSTimestamp());
          message.setJMSExpiration(pulsarMessage.getJMSExpiration());
          message.setJMSMessageID(pulsarMessage.getJMSMessageID());
        });
  }

  @Override
  public Queue getQueue() throws JMSException {
    checkNotClosed();
    if (defaultDestination.isQueue()) {
      return (Queue) defaultDestination;
    }
    throw new JMSException("Created on a topic");
  }

  @Override
  public void send(Queue queue, Message message) throws JMSException {
    this.send((Destination) queue, message);
  }

  @Override
  public void send(Queue queue, Message message, int i, int i1, long l) throws JMSException {
    this.send((Destination) queue, message, i, i1, l);
  }

  @Override
  public Topic getTopic() throws JMSException {
    checkNotClosed();
    if (defaultDestination.isTopic()) {
      return (Topic) defaultDestination;
    }
    throw new JMSException("Created on a queue");
  }

  @Override
  public void publish(Message message) throws JMSException {
    send(message);
  }

  @Override
  public void publish(Message message, int i, int i1, long l) throws JMSException {
    send(message, i, i1, l);
  }

  @Override
  public void publish(Topic topic, Message message) throws JMSException {
    send((Destination) topic, message);
  }

  @Override
  public void publish(Topic topic, Message message, int i, int i1, long l) throws JMSException {
    send((Destination) topic, message, i, i1, l);
  }
}
