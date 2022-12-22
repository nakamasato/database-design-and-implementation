package simpledb.jdbc.network;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import simpledb.jdbc.DriverAdapter;

/*
 * The SimpleDB database driver
 */
public class NetworkDriver extends DriverAdapter {

  /*
   * Connect to the SimpleDB server on the specified host.
   * The method retrieves the RemoteDriver stub from
   * the RMI registry on the specified host.
   * It calls the connect method on the stub,
   * which in turn creates a new connection and returns
   * its corresponding RemoteConnection stub.
   */
  public Connection connect(String url, Properties prop) throws SQLException {
    try {
      String host = url.replace("jdbc:simpledb://", "");
      Registry reg = LocateRegistry.getRegistry(host, 1099);
      RemoteDriver rdvr = (RemoteDriver) reg.lookup("simpledb");
      RemoteConnection rconn = rdvr.connect();
      return new NetworkConnection(rconn);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
