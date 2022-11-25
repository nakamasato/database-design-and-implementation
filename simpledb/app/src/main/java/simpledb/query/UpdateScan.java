package simpledb.query;

import simpledb.record.RID;

/*
 * The interface will be implemented by all updatable scans.
 */
public interface UpdateScan extends Scan {

  /*
   * Modify the field value of the current record.
   */
  public void setVal(String fldname, Constant val);

  /*
   * Modify the field value of the current record.
   */
  public void setInt(String fldname, int val);

  /*
   * Modify the field value of the current record.
   */
  public void setString(String fldname, String val);

  public void insert();

  public void delete();

  public RID getRid();

  public void moveToRid(RID rid);
}
