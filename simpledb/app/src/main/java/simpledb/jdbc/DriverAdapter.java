package simpledb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/*
 * This class implements all of the methods of the Driver interface,
 * by throwing an exception for each one.
 * Subclasses (such as SimpleDriver) can override them.
 */
public abstract class DriverAdapter implements Driver {

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    throw new SQLException("operation not implemented");
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    throw new SQLException("operation not implemented");
  }

  @Override
  public int getMajorVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMinorVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("operation not implemented");
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean jdbcCompliant() {
    // TODO Auto-generated method stub
    return false;
  }
}
