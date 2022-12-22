package simpledb.jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

/*
 * The RMI remote interface corresponding to Driver.
 * The method is similar to that of Driver,
 * except that it takes no arguments and
 * throws RemoteException instead of SQLException
 */
public interface RemoteDriver extends Remote {
  public RemoteConnection connect() throws RemoteException;
}
