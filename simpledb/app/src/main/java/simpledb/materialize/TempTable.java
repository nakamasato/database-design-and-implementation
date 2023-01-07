package simpledb.materialize;

import simpledb.query.UpdateScan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class TempTable {
  private static int nextTableNum = 0;
  private Transaction tx;
  private String tblname;
  private Layout layout;

  public TempTable(Transaction tx, Schema sch) {
    this.tx = tx;
    tblname = nextTableName();
    layout = new Layout(sch);
  }

  public UpdateScan open() {
    return new TableScan(tx, tblname, layout);
  }

  public String tableName() {
    return tblname;
  }

  public Layout getLayout() {
    return layout;
  }

  private static synchronized String nextTableName() {
    nextTableNum++;
    return "temp" + nextTableNum;
  }
}
