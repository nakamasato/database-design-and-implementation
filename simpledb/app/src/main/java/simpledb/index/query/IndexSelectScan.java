package simpledb.index.query;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.record.TableScan;

public class IndexSelectScan implements Scan {
  private TableScan ts;
  private Index idx;
  private Constant val;

  public IndexSelectScan(TableScan ts, Index idx, Constant val) {
    this.ts = ts;
    this.idx = idx;
    this.val = val;
    beforeFirst();
  }

  @Override
  public void beforeFirst() {
    idx.beforeFirst(val);
  }

  @Override
  public boolean next() {
    boolean ok = idx.next();
    if (ok) {
      RID rid = idx.getDataRid();
      ts.moveToRid(rid);
    }
    return ok;
  }

  @Override
  public int getInt(String fldname) {
    return ts.getInt(fldname);
  }

  @Override
  public String getString(String fldname) {
    return ts.getString(fldname);
  }

  @Override
  public Constant getVal(String fldname) {
    return ts.getVal(fldname);
  }

  @Override
  public boolean hasField(String fldname) {
    return ts.hasField(fldname);
  }

  @Override
  public void close() {
    idx.close();
    ts.close();
  }
}
