package simpledb.client.network;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.jdbc.network.NetworkDriver;

public class CreateStudentDB {
  public static void main(String[] args) {
    Driver d = new NetworkDriver();
    String url = "jdbc:simpledb://localhost";
    try (Connection conn = d.connect(url, null);
        Statement stmt = conn.createStatement()) {
      String s = "create table STUDENT (Sid int, SName varchar(10), MajorId int, GradYear int)";
      stmt.executeUpdate(s);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
