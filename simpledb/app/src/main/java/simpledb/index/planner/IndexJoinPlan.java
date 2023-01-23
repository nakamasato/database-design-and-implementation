package simpledb.index.planner;

import simpledb.index.Index;
import simpledb.index.query.IndexJoinScan;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class IndexJoinPlan implements Plan {
  private Plan p1;
  private Plan p2;
  private IndexInfo ii;
  private String joinfield;
  private Schema sch = new Schema();

  public IndexJoinPlan(Plan p1, Plan p2, IndexInfo ii, String joinfield) {
    this.p1 = p1;
    this.p2 = p2;
    this.ii = ii;
    this.joinfield = joinfield;
    sch.addAll(p1.schema());
    sch.addAll(p2.schema());
  }

  /*
   * RHS: TableScan
   * LHS: index
   */
  @Override
  public Scan open() {
    Scan s = p1.open();
    TableScan ts = (TableScan) p2.open();
    Index idx = ii.open();
    return new IndexJoinScan(s, idx, joinfield, ts);
  }

  /*
   * B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx) + R(idexjoin(p1,p2,idx))
   */
  @Override
  public int blockAccessed() {
    return p1.blockAccessed()
        + (p1.recordsOutput() * ii.blocksAccessed())
        + recordsOutput();
  }

  /*
   * R(indexjoin(p1,p2,idx)) = R(p1)*R(idx)
   */
  @Override
  public int recordsOutput() {
    return p1.recordsOutput() * ii.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    if (p1.schema().hasField(fldname))
      return p1.distinctValues(fldname);
    else
      return p2.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return sch;
  }

  @Override
  public int preprocessingCost() {
    return 0;
  }
}
