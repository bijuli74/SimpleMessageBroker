package org.example;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

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

  private void handleConsumer(ObjectInputStream in, ObjectOutputStream out, Socket clientSocket) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'handleConsumer'");
  }

  private void handleProducer(ObjectInputStream in) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'handleProducer'");
  }
}
