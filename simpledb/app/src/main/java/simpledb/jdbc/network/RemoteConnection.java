package simpledb.jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

/*
 * The RMI remote interface corresponding to Connection.
 * The methods are identical to those of Connection,
 * except that they throw RemoteException instead of SQLException.
 */
public interface RemoteConnection extends Remote {
  public RemoteStatement createStatement() throws RemoteException;

  public void close() throws RemoteException;
}
