package simpledb.query;

import simpledb.record.RID;

/*
 * Select ralational algebra operator.
 * All methods except nect delegate their work to the underlying scan.
 */
public class SelectScan implements UpdateScan {
  private Scan s;
  private Predicate pred;

  public SelectScan(Scan s, Predicate pred) {
    this.s = s;
    this.pred = pred;
  }

  @Override
  public void beforeFirst() {
    s.beforeFirst();
  }

  @Override
  public boolean next() {
    while (s.next())
      if (pred.isSatisfied(s))
        return true;
    return false;
  }

  @Override
  public int getInt(String fldname) {
    return s.getInt(fldname);
  }

  @Override
  public String getString(String fldname) {
    return s.getString(fldname);
  }

  @Override
  public Constant getVal(String fldname) {
    return s.getVal(fldname);
  }

  @Override
  public boolean hasField(String fldname) {
    return s.hasField(fldname);
  }

  @Override
  public void close() {
    s.close();
  }

  @Override
  public void setVal(String fldname, Constant val) {
    UpdateScan us = (UpdateScan) s;
    us.setVal(fldname, val);
  }

  @Override
  public void setInt(String fldname, int val) {
    UpdateScan us = (UpdateScan) s;
    us.setInt(fldname, val);
  }

  @Override
  public void setString(String fldname, String val) {
    UpdateScan us = (UpdateScan) s;
    us.setString(fldname, val);
  }

  @Override
  public void insert() {
    UpdateScan us = (UpdateScan) s;
    us.insert();
  }

  @Override
  public void delete() {
    UpdateScan us = (UpdateScan) s;
    us.delete();
  }

  @Override
  public RID getRid() {
    UpdateScan us = (UpdateScan) s;
    return us.getRid();
  }

  @Override
  public void moveToRid(RID rid) {
    UpdateScan us = (UpdateScan) s;
    us.moveToRid(rid);
  }
}
