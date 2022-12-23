package simpledb.jdbc.embedded;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import simpledb.jdbc.ResultSetAdapter;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class EmbeddedResultSet extends ResultSetAdapter {
  private Scan s;
  private Schema sch;
  private EmbeddedConnection conn;

  public EmbeddedResultSet(Plan plan, EmbeddedConnection conn) throws SQLException {
    s = plan.open();
    sch = plan.schema();
    this.conn = conn;
  }

  public boolean next() throws SQLException {
    try {
      return s.next();
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException(e);
    }
  }

  public int getInt(String fldname) throws SQLException {
    try {
      fldname = fldname.toLowerCase();
      return s.getInt(fldname);
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException(e);
    }
  }

  public String getString(String fldname) throws SQLException {
    try {
      fldname = fldname.toLowerCase();
      return s.getString(fldname);
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException(e);
    }
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    return new EmbeddedMetaData(sch);
  }
}
