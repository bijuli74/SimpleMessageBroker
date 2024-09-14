package org.example;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Message implements Serializable {

  private static final long serialVersionUID = 1L;
  private final String topic;
  private final String body;
  private final LocalDateTime timestamp;

  public Message(String topic, String body) {
    this.topic = topic;
    this.body = body;
    this.timestamp = LocalDateTime.now();
  }

  public static long getSerialversionuid() {
    return serialVersionUID;
  }

  public String getTopic() {
    return topic;
  }

  public String getBody() {
    return body;
  }

  public LocalDateTime getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "Message{" + "topic='" + topic + '\'' + ", body='" + body + '\'' + ", timestamp=" + timestamp + '}';
  }

}