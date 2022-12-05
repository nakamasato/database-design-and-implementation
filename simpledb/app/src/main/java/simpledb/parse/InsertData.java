package simpledb.parse;

import java.util.List;

import simpledb.query.Constant;

public class InsertData {
  private String tblname;
  private List<String> flds;
  private List<Constant> vals;

  public InsertData(String tblname, List<String> flds, List<Constant> vals) {
    this.tblname = tblname;
    this.flds = flds;
    this.vals = vals;
  }

  public String tableName() {
    return tblname;
  }

  public List<String> fields() {
    return flds;
  }

  public List<Constant> vals() {
    return vals;
  }
}
