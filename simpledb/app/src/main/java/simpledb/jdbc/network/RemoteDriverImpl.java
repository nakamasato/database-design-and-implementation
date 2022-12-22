package simpledb.jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import simpledb.server.SimpleDB;

@SuppressWarnings("serial")
public class RemoteDriverImpl extends UnicastRemoteObject implements RemoteDriver {
  private SimpleDB db;

  public RemoteDriverImpl(SimpleDB db) throws RemoteException {
    this.db = db;
  }

  public RemoteConnection connect() throws RemoteException {
    return new RemoteConnectionImpl(db);
  }
}
