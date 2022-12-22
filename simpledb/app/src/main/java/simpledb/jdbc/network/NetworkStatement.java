package simpledb.jdbc.network;

import java.sql.ResultSet;
import java.sql.SQLException;

import simpledb.jdbc.StatementAdapter;

public class NetworkStatement extends StatementAdapter {
  private RemoteStatement rstmt;

  public NetworkStatement(RemoteStatement s) {
    rstmt = s;
  }

  public ResultSet executeQuery(String qry) throws SQLException {
    try {
      RemoteResultSet rrs = rstmt.executeQuery(qry);
      return new NetworkResultSet(rrs);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public int executeUpdate(String cmd) throws SQLException {
    try {
      return rstmt.executeUpdate(cmd);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void close() throws SQLException {
    try {
      rstmt.close();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
