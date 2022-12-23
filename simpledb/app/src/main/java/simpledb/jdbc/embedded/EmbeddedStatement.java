package simpledb.jdbc.embedded;

import java.sql.SQLException;

import simpledb.jdbc.StatementAdapter;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.tx.Transaction;

public class EmbeddedStatement extends StatementAdapter {
  private EmbeddedConnection conn;
  private Planner planner;

  public EmbeddedStatement(EmbeddedConnection conn, Planner planner) {
    this.conn = conn;
    this.planner = planner;
  }

  public EmbeddedResultSet executeQuery(String qry) throws SQLException {
    try {
      Transaction tx = conn.getTransaction();
      Plan pln = planner.createQueryPlan(qry, tx);
      return new EmbeddedResultSet(pln, conn);
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException(e);
    }
  }

  public int executeUpdate(String cmd) throws SQLException {
    try {
      Transaction tx = conn.getTransaction();
      int result = planner.executeUpdate(cmd, tx);
      conn.commit();
      return result;
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException();
    }
  }

  public void close() throws SQLException {
  }
}
