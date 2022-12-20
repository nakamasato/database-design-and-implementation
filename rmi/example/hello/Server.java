package example.hello;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Server implements Hello {
  public Server() {
  }

  public static void main(String args[]) {
    try {
      Server obj = new Server();
      Hello stub = (Hello) UnicastRemoteObject.exportObject(obj, 0);

      Registry registry = LocateRegistry.createRegistry(1099);
      registry.bind("Hello", stub);
      System.out.println("Server ready");
    } catch (Exception e) {
      System.err.println("Server exception: " + e.toString());
      e.printStackTrace();
    }
  }

  @Override
  public String sayHello() throws RemoteException {
    return "Hello, World!";
  }
}
