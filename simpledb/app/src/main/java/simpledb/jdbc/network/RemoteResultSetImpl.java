package simpledb.jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class RemoteResultSetImpl extends UnicastRemoteObject implements RemoteResultSet {
  private Scan s;
  private Schema sch;
  private RemoteConnectionImpl rconn;

  public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl rconn) throws RemoteException {
    s = plan.open();
    sch = plan.schema();
    this.rconn = rconn;
  }

  @Override
  public boolean next() throws RemoteException {
    try {
      return s.next();
    } catch (RuntimeException e) {
      rconn.rollback();
      throw e;
    }
  }

  @Override
  public int getInt(String fldname) throws RemoteException {
    try {
      fldname = fldname.toLowerCase();
      return s.getInt(fldname);
    } catch (RuntimeException e) {
      rconn.rollback();
      throw e;
    }
  }

  @Override
  public String getString(String fldname) throws RemoteException {
    try {
      fldname = fldname.toLowerCase();
      return s.getString(fldname);
    } catch (RuntimeException e) {
      rconn.rollback();
      throw e;
    }
  }

  @Override
  public RemoteMetaData getMetaData() throws RemoteException {
    return new RemoteMetaDataImpl(sch);
  }

  @Override
  public void close() throws RemoteException {
    s.close();
    rconn.commit();
  }

}
