package simpledb.jdbc.embedded;

import java.sql.SQLException;

import simpledb.jdbc.ConnectionAdapter;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class EmbeddedConnection extends ConnectionAdapter {
  private SimpleDB db;
  private Transaction currentTx;
  private Planner planner;

  public EmbeddedConnection(SimpleDB db) {
    this.db = db;
    currentTx = db.newTx();
    planner = db.planner();
  }

  public EmbeddedStatement createStatement() throws SQLException {
    return new EmbeddedStatement(this, planner);
  }

  public void close() throws SQLException {
    currentTx.commit();
  }

  public void commit() throws SQLException {
    currentTx.commit();
    currentTx = db.newTx();
  }

  public void rollback() throws SQLException {
    currentTx.rollback();
    currentTx = db.newTx();
  }

  Transaction getTransaction() {
    return currentTx;
  }
}
