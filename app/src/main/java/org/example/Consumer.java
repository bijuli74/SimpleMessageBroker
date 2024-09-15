package org.example;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Consumer {
  private final String brokerHost;
  private final int brokerPort;

  public Consumer(String brokerHost, int brokerPort) {
    this.brokerHost = brokerHost;
    this.brokerPort = brokerPort;
  }

  // Subscribe to a topic and Receive messsages
  public void subscribe(String topic) {
    try {
      Socket socket = new Socket(brokerHost, brokerPort);
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

      out.writeObject("consumer");

      // Subscribe to the topic
      out.writeObject(topic);
      System.out.println("Subsribed to topic: " + topic);

      // Wait for messages from the broker
      while (true) {
        Message msg = (Message) in.readObject();
        System.out.println("Received message: " + msg);
      }
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Consumer consumer = new Consumer("localhost", 9090);
    consumer.subscribe("topic1");
  }
}
