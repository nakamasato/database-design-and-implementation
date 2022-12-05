package simpledb.parse;

import simpledb.query.Predicate;

/*
 * Data for the SQL delete statement
 */
public class DeleteData {
  private String tblname;
  private Predicate pred;

  public DeleteData(String tblname, Predicate pred) {
    this.tblname = tblname;
    this.pred = pred;
  }

  public String tableName() {
    return tblname;
  }

  public Predicate pred() {
    return pred;
  }
}
