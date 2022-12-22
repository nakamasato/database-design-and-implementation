package simpledb.jdbc.network;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import simpledb.jdbc.ResultSetAdapter;

public class NetworkResultSet extends ResultSetAdapter {
  private RemoteResultSet rrs;

  public NetworkResultSet(RemoteResultSet s) {
    rrs = s;
  }

  public boolean next() throws SQLException {
    try {
      return rrs.next();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public int getInt(String fldname) throws SQLException {
    try {
      return rrs.getInt(fldname);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public String getString(String fldname) throws SQLException {
    try {
      return rrs.getString(fldname);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public ResultSetMetaData getMetadata() throws SQLException {
    try {
      RemoteMetaData rmd = rrs.getMetaData();
      return new NetworkMetaData(rmd);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
