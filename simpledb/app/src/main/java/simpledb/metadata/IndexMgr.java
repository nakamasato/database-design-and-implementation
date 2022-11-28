package simpledb.metadata;

import static simpledb.metadata.TableMgr.MAX_NAME;

import java.util.HashMap;
import java.util.Map;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class IndexMgr {
  private Layout layout;
  private TableMgr tblmgr;
  private StatMgr statmgr;
  private static final String IDX_CAT_TABLE_NAME = "idxcat";
  private static final String IDX_CAT_FIELD_INDEX_NAME = "indexname";
  private static final String IDX_CAT_FIELD_TABLE_NAME = "tablename";
  private static final String IDX_CAT_FIELD_FIELD_NAME = "fieldname";

  public IndexMgr(boolean isnew, TableMgr tblmgr, StatMgr statmgr, Transaction tx) {
    if (isnew) {
      Schema sch = new Schema();
      sch.addStringField(IDX_CAT_FIELD_INDEX_NAME, MAX_NAME);
      sch.addStringField(IDX_CAT_FIELD_TABLE_NAME, MAX_NAME);
      sch.addStringField(IDX_CAT_FIELD_FIELD_NAME, MAX_NAME);
    }
    this.tblmgr = tblmgr;
    this.statmgr = statmgr;
    layout = tblmgr.getLayout(IDX_CAT_FIELD_TABLE_NAME, tx);
  }

  public void creatIndex(String idxname, String tblname, String fldname, Transaction tx) {
    TableScan ts = new TableScan(tx, IDX_CAT_TABLE_NAME, layout);
    ts.insert();
    ts.setString(IDX_CAT_FIELD_INDEX_NAME, idxname);
    ts.setString(IDX_CAT_FIELD_TABLE_NAME, tblname);
    ts.setString(IDX_CAT_FIELD_FIELD_NAME, fldname);
    ts.close();
  }

  public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
    Map<String, IndexInfo> result = new HashMap<>();
    TableScan ts = new TableScan(tx, IDX_CAT_TABLE_NAME, layout);
    while (ts.next()) {
      if (ts.getString(IDX_CAT_FIELD_TABLE_NAME).equals(tblname)) {
        String idxname = ts.getString(IDX_CAT_FIELD_INDEX_NAME);
        String fldname = ts.getString(IDX_CAT_FIELD_FIELD_NAME);
        Layout tblLayout = tblmgr.getLayout(tblname, tx);
        StatInfo tblsi = statmgr.getStatInfo(tblname, tblLayout, tx);
        IndexInfo ii = new IndexInfo(idxname, fldname, tblLayout.schema(), tx, tblsi);
        result.put(fldname, ii);
      }
    }
    return result;
  }
}
