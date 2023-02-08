package simpledb.multibuffer;

import simpledb.materialize.MaterializePlan;
import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class MultibufferProductPlan implements Plan {
  private Transaction tx;
  private Plan lhs;
  private Plan rhs;
  private Schema schema = new Schema();

  public MultibufferProductPlan(Transaction tx, Plan lhs, Plan rhs) {
    this.tx = tx;
    this.lhs = new MaterializePlan(tx, lhs);
    this.rhs = rhs;
    schema.addAll(lhs.schema());
    schema.addAll(rhs.schema());
  }

  /*
   * 1. Materialize LHS and RHS
   * 2. Determine the optimal chunk size
   * 3. Create a chunk plan for each chunk, and save them in a list.
   * 4. Create multiscan for the list.
   */
  @Override
  public Scan open() {
    Scan leftscan = lhs.open();
    TempTable tt = copyRecordsFrom(rhs);
    return new MultibufferProductScan(tx, leftscan, tt.tableName(), tt.getLayout());
  }

  /*
   * Calculate the numchunks by estimating the size of materialized right-side
   * table.
   * The estimation is provided by MaterializePlan
   */
  @Override
  public int blockAccessed() {
    int avail = tx.availableBuffs();
    int size = new MaterializePlan(tx, rhs).blockAccessed();
    int numchunks = size / avail;
    return rhs.blockAccessed() + (lhs.blockAccessed() * numchunks);
  }

  @Override
  public int recordsOutput() {
    return lhs.recordsOutput() * rhs.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    if (lhs.schema().hasField(fldname))
      return lhs.distinctValues(fldname);
    else
      return rhs.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return schema;
  }

  private TempTable copyRecordsFrom(Plan p) {
    Scan src = p.open();
    Schema sch = p.schema();
    TempTable t = new TempTable(tx, sch);
    UpdateScan dest = t.open();
    while (src.next()) {
      dest.insert();
      for (String fldname : sch.fields())
        dest.setVal(fldname, src.getVal(fldname));
    }
    src.close();
    dest.close();
    return t;
  }

  /*
   * Ref: 14.6.
   * 1. Materialize LHS (MaterializePlan)
   * 2. Read and write RHS (copyRecordsFrom)
   */
  @Override
  public int preprocessingCost() {
    return (lhs.preprocessingCost() // materialize preprocessing
        + rhs.blockAccessed() // read from rhs (input cost)
        + blockAccessed()); // write to temp table
  }
}
