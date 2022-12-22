package simpledb.server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import simpledb.jdbc.network.RemoteDriver;
import simpledb.jdbc.network.RemoteDriverImpl;

public class StartServer {

  public static void main(String args[]) throws Exception {
    // Init SimpleDB
    String dirname = (args.length == 0) ? "datadir" : args[0];
    SimpleDB db = new SimpleDB(dirname);

    // Create RMI registry
    Registry reg = LocateRegistry.createRegistry(1099);

    // Post the server entry
    RemoteDriver d = new RemoteDriverImpl(db);
    reg.rebind("simpledb", d);

    System.out.println("database server's ready");
  }
}
