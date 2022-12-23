package simpledb.jdbc.embedded;

import static java.sql.Types.INTEGER;

import java.sql.SQLException;

import simpledb.jdbc.ResultSetMetaDataAdapter;
import simpledb.record.Schema;

public class EmbeddedMetaData extends ResultSetMetaDataAdapter {
  private Schema sch;

  public EmbeddedMetaData(Schema sch) {
    this.sch = sch;
  }

  public String getColumnName(int column) throws SQLException {
    return sch.fields().get(column - 1);
  }

  public int getColumnType(int column) throws SQLException {
    String fldname = getColumnName(column);
    return sch.type(fldname);
  }

  public int getColumnDisplaySize(int column) throws SQLException {
    String fldname = getColumnName(column);
    int fldtype = sch.type(fldname);
    int fldlength = (fldtype == INTEGER) ? 6 : sch.length(fldname);
    return Math.max(fldname.length(), fldlength) + 1;
  }
}
