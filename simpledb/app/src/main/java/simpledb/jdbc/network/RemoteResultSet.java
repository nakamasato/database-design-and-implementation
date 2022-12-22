package simpledb.jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

/*
 * The RMI remote interface corresponding to ResultSet.
 * The methods are identical to those of ResultSet,
 * except that they throw RemoteException instead of SQLException.
 */
public interface RemoteResultSet extends Remote {
  public boolean next() throws RemoteException;

  public int getInt(String fldname) throws RemoteException;

  public String getString(String fldname) throws RemoteException;

  public RemoteMetaData getMetaData() throws RemoteException;

  public void close() throws RemoteException;
}
