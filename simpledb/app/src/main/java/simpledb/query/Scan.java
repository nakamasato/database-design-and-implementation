package simpledb.query;

/*
 * This interface will be implemented by each query scan.
 * There is a Scan class foreach relational algebra operator.
 */
public interface Scan {

  /*
   * Position
   * A subsequent call to next() will return the first record.
   */
  public void beforeFirst();

  /*
   * Move the scan to the next record.
   */
  public boolean next();

  /*
   * Return the value of the specified integer field
   * in the current record.
   */
  public int getInt(String fldname);

  /*
   * Return the value of the specified string field
   * in the current record.
   */
  public String getString(String fldname);

  /*
   * Return the value of the specified field in the current record.
   */
  public Constant getVal(String fldname);

  public boolean hasField(String fldname);

  /*
   * Close the scan and its subscans, if any.
   */
  public void close();
}
