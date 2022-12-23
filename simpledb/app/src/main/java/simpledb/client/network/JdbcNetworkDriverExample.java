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
