package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class MaterializePlan implements Plan {
  private Plan srcplan;
  private Transaction tx;

  public MaterializePlan(Transaction tx, Plan srcplan) {
    this.srcplan = srcplan;
    this.tx = tx;
  }

  @Override
  public Scan open() {
    Schema sch = srcplan.schema();
    TempTable temp = new TempTable(tx, sch);
    Scan src = srcplan.open();
    UpdateScan dest = temp.open();
    int cnt = 0;
    while (src.next()) {
      dest.insert();
      for (String fldname : sch.fields())
        dest.setVal(fldname, src.getVal(fldname));
      cnt++;
    }
    System.out.println("[MaterializePlan] inserted " + cnt + " records to TempTable[" + temp.tableName() + "]");
    src.close();
    dest.beforeFirst();
    return dest;
  }

  /*
   * Return the estimated number of block access, which
   * doesn't include the one-time cost of materializing the records
   * (preprocessing cost).
   */
  @Override
  public int blockAccessed() {
    Layout layout = new Layout(srcplan.schema());
    double rpb = tx.blockSize() / layout.slotSize();
    return (int) Math.ceil(srcplan.recordsOutput() / rpb);
  }

  @Override
  public int recordsOutput() {
    return srcplan.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    return srcplan.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return srcplan.schema();
  }

}
