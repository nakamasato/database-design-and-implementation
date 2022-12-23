## Chapter 11: JDBC Interfaces

### 11.1. SimpleDB

1. Create `SimpleDB.java`

    ```java
    package simpledb.server;

    import java.io.File;

    import simpledb.buffer.BufferMgr;
    import simpledb.file.FileMgr;
    import simpledb.log.LogMgr;
    import simpledb.metadata.MetadataMgr;
    import simpledb.plan.BasicQueryPlanner;
    import simpledb.plan.BasicUpdatePlanner;
    import simpledb.plan.Planner;
    import simpledb.plan.QueryPlanner;
    import simpledb.plan.UpdatePlanner;
    import simpledb.tx.Transaction;

    public class SimpleDB {
      public static int BLOCK_SIZE = 400;
      public static int BUFFER_SIZE = 8;
      public static String LOG_FILE = "simpledb.log";

      private FileMgr fm;
      private BufferMgr bm;
      private LogMgr lm;
      private MetadataMgr mdm;
      private Planner planner;

      /*
       * A constructor useful for debugging
       */
      public SimpleDB(String dirname, int blocksize, int buffsize) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, blocksize);
        lm = new LogMgr(fm, LOG_FILE);
        bm = new BufferMgr(fm, lm, buffsize);
      }

      /*
       * Simple constructor
       */
      public SimpleDB(String dirname) {
        this(dirname, BLOCK_SIZE, BUFFER_SIZE);
        Transaction tx = newTx();
        boolean isnew = fm.isNew();
        if (isnew)
          System.out.println("creating new database");
        else {
          System.out.println("recovering existing database");
          tx.recover();
        }
        mdm = new MetadataMgr(isnew, tx);
        QueryPlanner qp = new BasicQueryPlanner(mdm);
        UpdatePlanner up = new BasicUpdatePlanner(mdm);
        planner = new Planner(qp, up);
        tx.commit();
      }

      public Transaction newTx() {
        return new Transaction(fm, lm, bm);
      }

      public MetadataMgr mdMgr() {
        return mdm;
      }

      public Planner planner() {
        return planner;
      }

      // These methods are for debugging
      public FileMgr fileMgr() {
        return fm;
      }

      public LogMgr logMgr() {
        return lm;
      }

      public BufferMgr bufferMgr() {
        return bm;
      }
    }
    ```

1. Add `isNew()` to `file/FileMgr.java`

    ```java
    public boolean isNew() {
      return isNew;
    }
    ```

### 11.2. Remote Method Invoction (RMI)

You can practice with a quick start in [RMI](../../rmi/README.md).

### 11.3. Five JDBC Interfaces (server-based)

![](jdbc-interfaces.drawio.svg)

To make remote access possible for `java.sql.Driver`, `java.sql.Connection`, `java.sql.Statement`, `java.sql.ResultSet`, `java.sql.ResultSetMetaData`, we need to combine them with `java.rmi.Remote` interface:

1. `Driver` (interface) <- `DriverAdapter` (abstract) <- `NetworkDriver` (class) gets `RemoteDriver` from rmi registry, which enables to communicate with remote server.
    1. `Remote` (interface) <- `RemoteDriver` (interface) <- `RemoteDriverImpl`
1. `Connection` (interface) <- `ConnectionAdapter` (abstract) <- `NetworkConnection` having `RemoteConnection`
    1. `Remote` (interface) <- `RemoteConnection` (interface) <- `RemoteConnectionImpl`
1. `Statement` (interface) <- `StatementAdapter` (abstract) <- `NetworkStatement` (class) having `RemoteStatement` which enables to communicate with remote server.
    1. `Remote` (interface) <- `RemoteStatement` (interface) <- `RemoteStatementImpl`
1. `ResultSet` (interface) <- `ResultSetAdapter` (abstract) <- `NetworkResultSet` having `RemoteResultSet`
    1. `Remote` (interface) <- `RemoteResultSet` (interface) <- `RemoteResultSetImpl`
1. `ResultSetMetaData` (interface) <- `ResultSetMetaDataAdapter` (abstract) <- `NetworkMetaData` having `RemoteMetaData`
    1. `Remote` (interface) <- `RemoteMetaData` (interface) <- `RemoteMetaDataImpl`


#### 11.3.1. Driver
1. Add `jdbc/DriverAdapter.java`

    ```java
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
    ```
1. Add `jdbc/network/NetworkDriver.java`

    ```java
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
    ```

Requires `NetworkConnection`

#### 11.3.2. Connection

1. Add `jdbc/ConnectionAdapter.java`

    Throw `SQLException` in all the methods.

    ```java
    package simpledb.jdbc;

    import java.sql.Array;
    import java.sql.Blob;
    import java.sql.CallableStatement;
    import java.sql.Clob;
    import java.sql.Connection;
    import java.sql.DatabaseMetaData;
    import java.sql.NClob;
    import java.sql.PreparedStatement;
    import java.sql.SQLClientInfoException;
    import java.sql.SQLException;
    import java.sql.SQLWarning;
    import java.sql.SQLXML;
    import java.sql.Savepoint;
    import java.sql.Statement;
    import java.sql.Struct;
    import java.util.Map;
    import java.util.Properties;
    import java.util.concurrent.Executor;

    public abstract class ConnectionAdapter implements Connection {

      @Override
      public void abort(Executor executor) throws SQLException {
        throw new SQLException("operation not implemented");
      }
      // ...
    }
    ```
1. Add `jdbc/network/NetworkConnection.java`
    ```java
    package simpledb.jdbc.network;

    import java.sql.SQLException;
    import java.sql.Statement;

    import simpledb.jdbc.ConnectionAdapter;

    public class NetworkConnection extends ConnectionAdapter {
      private RemoteConnection rconn;

      public NetworkConnection(RemoteConnection c) {
        rconn = c;
      }

      public Statement createStatement() throws SQLException {
        try {
          RemoteStatement rstmt = rconn.createStatement();
          return new NetworkStatement(rstmt);
        } catch (Exception e) {
          throw new SQLException(e);
        }
      }

      public void close() throws SQLException {
        try {
          rconn.close();
        } catch (Exception e) {
          throw new SQLException(e);
        }
      }
    }
    ```
1. Add `jdbc/network/RemoteConnection.java`
    ```java
    package simpledb.jdbc.network;

    import java.rmi.Remote;
    import java.rmi.RemoteException;

    /*
     * The RMI remote interface corresponding to Connection.
     * The methods are identical to those of Connection,
     * except that they throw RemoteException instead of SQLException.
     */
    public interface RemoteConnection extends Remote {
      public RemoteStatement createStatement() throws RemoteException;

      public void close() throws RemoteException;
    }
    ```

#### 11.3.3. Statement

1. Add `jdbc/StatementAdapter.java`
1. Add `jdbc/network/NetworkStatement.java`
1. Add `jdbc/network/RemoteStatement.java`

#### 11.3.4. ResultSet

1. Add `jdbc/ResultSetAdapter.java`
1. Add `jdbc/network/NetworkResultSet.java`
1. Add `jdbc/network/RemoteResultSet.java`

#### 11.3.4. ResultSetMetaData

1. Add `jdbc/ResultSetMetaDataAdapter.java`
1. Add `jdbc/network/NetworkMetaData.java`
1. Add `jdbc/network/RemoteMetaData.java`


#### 11.3.5. StartServer

1. Add `server/StartServer.java`

    ```java
    package simpledb.server;

    import java.rmi.registry.LocateRegistry;
    import java.rmi.registry.Registry;

    import simpledb.jdbc.network.RemoteDriver;
    import simpledb.jdbc.network.RemoteDriverImpl;

    public class StartServer {

      public static void main(String args[]) throws Exception {
        // Init SimpleDB
        String dirname = (args.length == 0) ? "datadir" : args[0];
        SimpleDB db = new SimpleDB(dirname);

        // Create RMI registry
        Registry reg = LocateRegistry.createRegistry(1099);

        // Post the server entry
        RemoteDriver d = new RemoteDriverImpl(db);
        reg.rebind("simpledb", d);

        System.out.println("database server's ready");
      }
    }
    ```

1. Add `jdbc/netwokr/RemoteDriverImpl.java`
1. Add `jdbc/network/RemoteConnectionImpl.java`
1. Add `jdbc/network/RemoteStatementImpl.java`
1. Add `jdbc/network/RemoteResultSetImpl.java`
1. Add `jdbc/network/RemoteMetaDataImpl.java`
#### 11.3.6. Client

1. Add `client/network/JdbcNetworkDriverExample.java`

    ```java
    package simpledb.client.network;

    import java.sql.Connection;
    import java.sql.Driver;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    import simpledb.jdbc.network.NetworkDriver;

    public class JdbcNetworkDriverExample {
      public static void main(String[] args) {
        Driver d = new NetworkDriver();
        String url = "jdbc:simpledb://localhost";
        try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {
          // 1. create table student
          String sql = "create table STUDENT (Sid int, SName varchar(10), MajorId int, GradYear int)";
          stmt.executeUpdate(sql);

          // 2. select tables
          sql = "select tblname, slotsize from tblcat";
          ResultSet rs = stmt.executeQuery(sql);
          while (rs.next())
            System.out.println(String.format("table: %s, slotsize: %d", rs.getString("tblname"), rs.getInt("slotsize")));

          // 3. insert record to student table
          sql = "insert into student(Sid, SName, MajorId, GradYear) values (1, 'John', 10, 2020)";
          stmt.executeUpdate(sql);

          // 4. select records from student table
          sql = "select Sid, SName, MajorId, GradYear from student";
          rs = stmt.executeQuery(sql);
          while (rs.next())
            System.out.println(String.format("Sid: %d, Sname: %s, MajorId: %d, GradYear: %d", rs.getInt("Sid"),
                rs.getString("SName"), rs.getInt("MajorId"), rs.getInt("GradYear")));
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
    ```

#### 11.3.7. Run

1. Add Gradle task to `build.gradle.kts`

    ```kts
    task("startServer", JavaExec::class) {
        group = "jdbc"
        main = "simpledb.server.StartServer"
        classpath = sourceSets["main"].runtimeClasspath
    }

    task("networkclient", JavaExec::class) {
        group = "jdbc"
        main = "simpledb.client.network.JdbcNetworkDriverExample"
        classpath = sourceSets["main"].runtimeClasspath
    }
    ```

1. Run server
    ```
    ./gradlew startServer
    ```

1. Run client

    ```
    ./gradlew networkclient
    ```

    ```
    table: tblcat, slotsize: 28
    table: fldcat, slotsize: 56
    table: viewcat, slotsize: 128
    table: student, slotsize: 30
    Sid: 1, Sname: John, MajorId: 10, GradYear: 2020
    ```

### 11.4. EmbeddedDriver

#### 11.4.1. Add five implementations
1. Add `java/jdbc/embedded/EmbeddedDriver.java`
1. Add `java/jdbc/embedded/EmbeddedConnection.java`
1. Add `java/jdbc/embedded/EmbeddedStatement.java`
1. Add `java/jdbc/embedded/EmbeddedResultSet.java`
1. Add `java/jdbc/embedded/EmbeddedMetaData.java`

#### 11.4.2. Add client

`java/client/network/JdbcEmbeddedDriverExample.java`

```java
package simpledb.client.network;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.jdbc.embedded.EmbeddedDriver;

public class JdbcEmbeddedDriverExample {
  public static void main(String[] args) {
    Driver d = new EmbeddedDriver();
    String url = "jdbc:simpledb:datadir";
    try (Connection conn = d.connect(url, null);
        Statement stmt = conn.createStatement()) {
      // 1. create table student
      String sql = "create table STUDENT (Sid int, SName varchar(10), MajorId int, GradYear int)";
      stmt.executeUpdate(sql);

      // 2. select tables
      sql = "select tblname, slotsize from tblcat";
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next())
        System.out.println(String.format("table: %s, slotsize: %d", rs.getString("tblname"), rs.getInt("slotsize")));

      // 3. insert record to student table
      sql = "insert into student(Sid, SName, MajorId, GradYear) values (1, 'John', 10, 2020)";
      stmt.executeUpdate(sql);

      // 4. select records from student table
      sql = "select Sid, SName, MajorId, GradYear from student";
      rs = stmt.executeQuery(sql);
      while (rs.next())
        System.out.println(String.format("Sid: %d, Sname: %s, MajorId: %d, GradYear: %d", rs.getInt("Sid"),
            rs.getString("SName"), rs.getInt("MajorId"), rs.getInt("GradYear")));
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
```

#### 11.4.2. Run

1. Add Gradle task to `build.gradle.kts`

    ```kts
    task("embeddedclient", JavaExec::class) {
        group = "jdbc"
        main = "simpledb.client.network.JdbcEmbeddedDriverExample"
        classpath = sourceSets["main"].runtimeClasspath
    }
    ```

1. Run client

    ```
    ./gradlew embeddedclient
    ```

### Error

```
lementation/simpledb ; /usr/bin/env /Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home/bi
n/java @/var/folders/c2/hjlk2kcn63s4kds9k2_ctdhc0000gp/T/cp_cslmdb7i94gc88sbkj2ehy4p3.argfile simpled
b.client.network.CreateStudentDB
java.sql.SQLException: java.lang.ClassCastException: class com.sun.proxy.$Proxy1 cannot be cast to class simpledb.jdbc.network.RemoteConnection (com.sun.proxy.$Proxy1 and simpledb.jdbc.network.RemoteConnection are in unnamed module of loader 'app')
        at simpledb.jdbc.network.NetworkDriver.connect(NetworkDriver.java:32)
        at simpledb.client.network.CreateStudentDB.main(CreateStudentDB.java:14)
Caused by: java.lang.ClassCastException: class com.sun.proxy.$Proxy1 cannot be cast to class simpledb.jdbc.network.RemoteConnection (com.sun.proxy.$Proxy1 and simpledb.jdbc.network.RemoteConnection are in unnamed module of loader 'app')
        at com.sun.proxy.$Proxy0.connect(Unknown Source)
        at simpledb.jdbc.network.NetworkDriver.connect(NetworkDriver.java:29)
        ... 1 more
```

-> Solution: Forgot to add `Remote` to the interfaces.
