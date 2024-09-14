package org.example;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

public class Broker {

  private final ConcurrentHashMap<String, Queue<Message>> messageQueues = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, List<Socket>> subscribers = new ConcurrentHashMap<>();

  public void start(int port) throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.err.println("Broker started on port: " + port);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        Thread.startVirtualThread(() -> handleClient(clientSocket));
      }
    }
  }

  private void handleClient(Socket clientSocket) {
    try {
      ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
      ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());

      String clientType = (String) in.readObject();

      if ("producer".equals(clientType)) {
        handleProducer(in);
      } else if ("consumer".equals(clientType)) {
        handleConsumer(in, out, clientSocket);
      }

    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  private void handleConsumer(ObjectInputStream in, ObjectOutputStream out, Socket socket)
      throws ClassNotFoundException, IOException {
    String topic = (String) in.readObject();

    subscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(socket);

    Queue<Message> queue = messageQueues.get(topic);
    if (queue != null) {
      for (Message msg : queue) {
        out.writeObject(msg);
      }
    }

  }

  private void handleProducer(ObjectInputStream in) throws ClassNotFoundException, IOException {
    Message msg = (Message) in.readObject();
    String topic = msg.getTopic();

    messageQueues.computeIfAbsent(topic, k -> new ConcurrentLinkedQueue<>()).add(msg);

    notifyConsumers(topic, msg);
  }

  private void notifyConsumers(String topic, Message msg) {
    List<Socket> topicSubscribers = subscribers.get(topic);

    if (topicSubscribers != null) {
      for (Socket subscriber : topicSubscribers) {
        Thread.startVirtualThread(() -> {
          try {
            ObjectOutputStream out = new ObjectOutputStream(subscriber.getOutputStream());
            out.writeObject(msg);

          } catch (IOException e) {
            e.printStackTrace();
          }
        });
      }
    }
  }

  public static void main(String[] args) throws IOException {
    Broker broker = new Broker();
    broker.start(9090);
  }
}
