package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;

/*
 * The interface implemented by each query plan.
 * There is a Plan class for each relational algebra operator.
 */
public interface Plan {
  /*
   * Open a scan corresponding to this plan
   */
  public Scan open();

  /*
   * The estimated number of block accesses
   * that will occur when the scan is executed.
   * This value is used to calculate the estimated cost of the plan
   */
  public int blockAccessed();

  /*
   * The estimated number of output records.
   * This value is used to calculate the estimated cost of the plan
   */
  public int recordsOutput();

  /*
   * The estimated number of distinct records for the specified field
   * This value is used to calculate the estimated cost of the plan
   */
  public int distinctValues(String fldname);

  /*
   * The estimated cost for preprocessing
   */
  public int preprocessingCost();

  /*
   * Schema of output table
   */
  public Schema schema();
}
