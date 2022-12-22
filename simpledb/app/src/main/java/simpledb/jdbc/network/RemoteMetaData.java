package simpledb.jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

/*
 * The RMI remote interface corresponding to ResultSetMetaData.
 * The methods are identical to those of ResultSetMetaData,
 * except that they throw RemoteException instead of SQLException.
 */
public interface RemoteMetaData extends Remote {
  public int getColumnCount() throws RemoteException;

  public String getColumnName(int column) throws RemoteException;

  public int getColumnType(int column) throws RemoteException;

  public int getColumnDisplaySize(int column) throws RemoteException;
}
