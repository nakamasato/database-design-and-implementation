package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class ViewMgr {
  private static final int MAX_VIEW_DEF = 100; // unrealistic for real case. clob(9999) would be better
  private static final String VIEW_CAT_TABLE = "viewcat";
  private static final String VIEW_CAT_FIELD_NAME = "viewname";
  private static final String VIEW_CAT_FIELD_DEF = "viewdef";

  TableMgr tblMgr;

  public ViewMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
    this.tblMgr = tblMgr;
    if (isNew) {
      Schema sch = new Schema();
      sch.addStringField(VIEW_CAT_FIELD_NAME, TableMgr.MAX_NAME);
      sch.addStringField(VIEW_CAT_FIELD_DEF, MAX_VIEW_DEF);
      tblMgr.createTable(VIEW_CAT_TABLE, sch, tx);
    }
  }

  public void createView(String vname, String vdef, Transaction tx) {
    Layout layout = tblMgr.getLayout(VIEW_CAT_TABLE, tx);
    TableScan ts = new TableScan(tx, VIEW_CAT_TABLE, layout);
    ts.insert();
    ts.setString(VIEW_CAT_FIELD_NAME, vname);
    ts.setString(VIEW_CAT_FIELD_DEF, vdef);
    ts.close();
  }

  public String getViewDef(String vname, Transaction tx) {
    String result = null;
    Layout layout = tblMgr.getLayout(VIEW_CAT_TABLE, tx);
    TableScan ts = new TableScan(tx, VIEW_CAT_TABLE, layout);
    while (ts.next()) {
      if (ts.getString(VIEW_CAT_FIELD_NAME).equals(vname)) {
        result = ts.getString(VIEW_CAT_FIELD_DEF);
        break;
      }
    }
    ts.close();
    return result;
  }
}
