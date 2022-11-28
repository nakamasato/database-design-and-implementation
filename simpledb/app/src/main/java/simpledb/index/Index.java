package simpledb.index;

import simpledb.query.Constant;
import simpledb.record.RID;

public interface Index {

  /*
   * Positions the index before the first record
   * having the specified search key
   */
  public void beforeFirst(Constant searchkey);

  /*
   * Moves the index to the next record having
   * the search key specified in the beforeFirst method
   */
  public boolean next();

  /*
   * Returns the dataRID value stored in the current index record
   */
  public RID getDataRid();

  /*
   * Inserts an index record having the specified dataval and dataRID values.
   */
  public void insert(Constant dataval, RID datarid);

  /*
   * Deletes the index record having the specified dataval and dataRID values.
   */
  public void delete(Constant dataval, RID datarid);

  /*
   * Closes the index.
   */
  public void close();

}
