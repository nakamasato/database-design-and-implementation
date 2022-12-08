package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.metadata.StatInfo;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class TablePlan implements Plan {
  private String tblname;
  private Transaction tx;
  private Layout layout;
  private StatInfo si;

  public TablePlan(Transaction tx, String tblname, MetadataMgr md) {
    this.tblname = tblname;
    this.tx = tx;
    layout = md.getLayout(tblname, tx);
    si = md.getStatInfo(tblname, layout, tx);
  }

  @Override
  public Scan open() {
    return new TableScan(tx, tblname, layout);
  }

  @Override
  public int blockAccessed() {
    return si.blocksAccessed();
  }

  @Override
  public int recordsOutput() {
    return si.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    return si.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return layout.schema();
  }
}
