package org.example;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Producer {
  private final String brokerHost;
  private final int brokerPort;

  public Producer(String brokerHost, int brokerPort) {
    this.brokerHost = brokerHost;
    this.brokerPort = brokerPort;
  }

  public void sendMessage(String topic, String body) {
    try {
      Socket socket = new Socket(brokerHost, brokerPort);
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      // ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

      // Identify as a producer
      out.writeObject("producer");

      Message msg = new Message(topic, body);
      out.writeObject(msg);
      System.out.println("Message sent: " + msg);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Producer producer = new Producer("localhost", 9090);
    producer.sendMessage("topic1", "Hello World!");
  }
}
