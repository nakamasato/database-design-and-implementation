package simpledb.jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

/*
 * The RMI remote interface corresponding to Statement.
 * The methods are identical to those of Statement,
 * except that they throw RemoteException instead of SQLException.
 */
public interface RemoteStatement extends Remote {
  public RemoteResultSet executeQuery(String qry) throws RemoteException;

  public int executeUpdate(String cmd) throws RemoteException;

  public void close() throws RemoteException;
}
