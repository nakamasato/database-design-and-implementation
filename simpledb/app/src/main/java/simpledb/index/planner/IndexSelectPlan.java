package simpledb.index.planner;

import simpledb.index.Index;
import simpledb.index.query.IndexSelectScan;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class IndexSelectPlan implements Plan {
  private Plan p;
  private IndexInfo ii;
  private Constant val;

  public IndexSelectPlan(Plan p, IndexInfo ii, Constant val) {
    this.p = p;
    this.ii = ii;
    this.val = val;
  }

  @Override
  public Scan open() {
    TableScan ts = (TableScan) p.open();
    Index idx = ii.open();
    return new IndexSelectScan(ts, idx, val);
  }

  @Override
  public int blockAccessed() {
    return ii.blocksAccessed() + recordsOutput();
  }

  @Override
  public int recordsOutput() {
    return ii.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    return ii.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return p.schema();
  }

  @Override
  public int preprocessingCost() {
    return 0;
  }
}
