package simpledb.jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.tx.Transaction;

@SuppressWarnings("serial")
public class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
  private RemoteConnectionImpl rconn;
  private Planner planner;

  public RemoteStatementImpl(RemoteConnectionImpl rconn, Planner planner) throws RemoteException {
    this.rconn = rconn;
    this.planner = planner;
  }

  @Override
  public RemoteResultSet executeQuery(String qry) throws RemoteException {
    try {
      Transaction tx = rconn.getTransaction();
      Plan pln = planner.createQueryPlan(qry, tx);
      return new RemoteResultSetImpl(pln, rconn);
    } catch (RuntimeException e) {
      rconn.rollback();
      throw e;
    }
  }

  @Override
  public int executeUpdate(String cmd) throws RemoteException {
    try {
      Transaction tx = rconn.getTransaction();
      int result = planner.executeUpdate(cmd, tx);
      rconn.commit();
      return result;
    } catch (RuntimeException e) {
      rconn.rollback();
      throw e;
    }
  }

  @Override
  public void close() throws RemoteException {
  }
}
