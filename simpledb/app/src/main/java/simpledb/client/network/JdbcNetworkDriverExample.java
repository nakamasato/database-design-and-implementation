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

      // 2. create index
      sql = "create index student_sid_idx on student(sid)";
      stmt.executeUpdate(sql);

      // 3. select tables
      sql = "select tblname, slotsize from tblcat";
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next())
        System.out.println(String.format("table: %s, slotsize: %d", rs.getString("tblname"), rs.getInt("slotsize")));

      // 4. insert record to student table
      for (int i = 1; i <= 100; i++) {
        int gradYear = 100 + i % 7;
        int majorId = i % 13;
        String name = "name" + i;
        sql = String.format("insert into student(Sid, SName, MajorId, GradYear) values (%d, '%s', %d, %d)",
            i, name, majorId, gradYear);
        stmt.executeUpdate(sql);
      }

      // 5. select records from student table
      System.out.println("select all records from student table ------");
      sql = "select Sid, SName, MajorId, GradYear from student";
      rs = stmt.executeQuery(sql);
      while (rs.next())
        System.out.println(String.format("Sid: %d, Sname: %s, MajorId: %d, GradYear: %d", rs.getInt("Sid"),
            rs.getString("SName"), rs.getInt("MajorId"), rs.getInt("GradYear")));

      // 6. select records from student table with condition
      System.out.println("select records (sid =1000) from student table ------");
      sql = "select Sid, SName, MajorId, GradYear from student where sid = 1000";
      rs = stmt.executeQuery(sql);
      while (rs.next())
        System.out.println(String.format("Sid: %d, Sname: %s, MajorId: %d, GradYear: %d", rs.getInt("Sid"),
            rs.getString("SName"), rs.getInt("MajorId"), rs.getInt("GradYear")));
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
