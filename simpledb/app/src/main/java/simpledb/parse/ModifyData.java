package simpledb.parse;

import simpledb.query.Expression;
import simpledb.query.Predicate;

/*
 * Data for the SQL update statement
 */
public class ModifyData {
  private String tblname;
  private String fldname;
  private Expression newval;
  private Predicate pred;

  public ModifyData(String tblname, String fldname, Expression newval, Predicate pred) {
    this.tblname = tblname;
    this.fldname = fldname;
    this.newval = newval;
    this.pred = pred;
  }

  public String tableName() {
    return tblname;
  }

  public String targetField() {
    return fldname;
  }

  public Expression newValue() {
    return newval;
  }

  public Predicate pred() {
    return pred;
  }
}
