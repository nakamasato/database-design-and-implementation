package simpledb.index.planner;

import java.util.Iterator;
import java.util.Map;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.CreateIndexData;
import simpledb.parse.CreateTableData;
import simpledb.parse.CreateViewData;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.ModifyData;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.plan.UpdatePlanner;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.tx.Transaction;

public class IndexUpdatePlanner implements UpdatePlanner {
  private MetadataMgr mdm;

  public IndexUpdatePlanner(MetadataMgr mdm) {
    this.mdm = mdm;
  }

  @Override
  public int executeInsert(InsertData data, Transaction tx) {
    String tblname = data.tableName();
    Plan p = new TablePlan(tx, tblname, mdm);

    // first, insert the record
    UpdateScan s = (UpdateScan) p.open();
    s.insert();
    RID rid = s.getRid();

    // then insert an index record for every index
    Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);
    Iterator<Constant> valIter = data.vals().iterator();
    for (String fldname : data.fields()) {
      Constant val = valIter.next();
      s.setVal(fldname, val);

      IndexInfo ii = indexes.get(fldname);
      if (ii != null) {
        Index idx = ii.open();
        idx.insert(val, rid);
        idx.close();
      }
    }
    s.close();
    return 1;
  }

  @Override
  public int executeDelete(DeleteData data, Transaction tx) {
    String tblname = data.tableName();
    Plan p = new TablePlan(tx, tblname, mdm);
    Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);

    UpdateScan s = (UpdateScan) p.open();
    int count = 0;
    while (s.next()) {
      // first delete index from every index
      RID rid = s.getRid();
      for (Map.Entry<String, IndexInfo> entry : indexes.entrySet()) {
        Constant val = s.getVal(entry.getKey());
        Index idx = entry.getValue().open();
        idx.delete(val, rid);
        idx.close();
      }

      // then delete the record
      s.delete();
      count++;
    }
    s.close();
    return count;
  }

  @Override
  public int executeModify(ModifyData data, Transaction tx) {
    String tblname = data.tableName();
    String fldname = data.targetField();
    Plan p = new TablePlan(tx, tblname, mdm);
    p = new SelectPlan(p, data.pred());

    IndexInfo ii = mdm.getIndexInfo(tblname, tx).get(fldname);
    Index idx = (ii == null) ? null : ii.open();

    UpdateScan s = (UpdateScan) p.open();
    int count = 0;
    while (s.next()) {
      // first, update the record
      Constant newval = data.newValue().evaluate(s);
      Constant oldval = s.getVal(fldname);
      s.setVal(data.targetField(), newval);

      // then update the index if exists
      if (idx != null) {
        RID rid = s.getRid();
        idx.delete(oldval, rid);
        idx.insert(newval, rid);
      }
      count++;
    }
    if (idx != null)
      idx.close();
    s.close();
    return count;
  }

  @Override
  public int executeCreateTable(CreateTableData data, Transaction tx) {
    mdm.createTable(data.tableName(), data.newSchema(), tx);
    return 0;
  }

  @Override
  public int executeCreateView(CreateViewData data, Transaction tx) {
    mdm.createView(data.viewName(), data.viewDef(), tx);
    return 0;
  }

  @Override
  public int executeCreateIndex(CreateIndexData data, Transaction tx) {
    mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
    return 0;
  }
}
