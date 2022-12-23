package simpledb.jdbc.embedded;

import java.sql.SQLException;
import java.util.Properties;

import simpledb.jdbc.DriverAdapter;
import simpledb.server.SimpleDB;

public class EmbeddedDriver extends DriverAdapter {

  public EmbeddedConnection connect(String url, Properties p) throws SQLException {
    String dbname = url.replace("jdbc:simpledb:", "");
    SimpleDB db = new SimpleDB(dbname);
    return new EmbeddedConnection(db);
  }
}
