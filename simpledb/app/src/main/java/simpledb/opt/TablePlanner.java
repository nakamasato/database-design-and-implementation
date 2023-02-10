package simpledb.opt;

import java.util.Map;
import java.util.Map.Entry;

import simpledb.index.planner.IndexJoinPlan;
import simpledb.index.planner.IndexSelectPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.Predicate;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/*
 * plan for a single table
 */
public class TablePlanner {
  private TablePlan myplan;
  private Predicate mypred;
  private Schema myschema;
  private Map<String, IndexInfo> indexes;
  private Transaction tx;

  public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
    this.mypred = mypred;
    this.tx = tx;
    myplan = new TablePlan(tx, tblname, mdm);
    myschema = myplan.schema();
    indexes = mdm.getIndexInfo(tblname, tx);
  }

  /*
   * Construct a select plan for the table.
   * Use IndexSelectPlan if possible.
   * Otherwise, use TablePlan
   */
  public Plan makeSelectPlan() {
    Plan p = makeIndexSelect();
    if (p == null)
      p = myplan;
    return addSelectPred(p);
  }

  /*
   * Construct a join plan for the specified plan and table
   * use an index join if possible.
   * return null if no join is possible
   */
  public Plan makeJoinPlan(Plan current) {
    Schema currsch = current.schema();
    Predicate joinpred = mypred.joinSubPred(myschema, currsch);
    if (joinpred == null)
      return null;
    Plan p = makeIndexJoin(current, currsch);
    if (p == null)
      p = makeProductJoin(current, currsch);
    return p;
  }

  public Plan makeProductPlan(Plan current) {
    Plan p = addSelectPred(myplan);
    return new MultibufferProductPlan(tx, current, p);
  }

  private Plan makeIndexSelect() {
    for (Entry<String, IndexInfo> entry : indexes.entrySet()) {
      String fldname = entry.getKey();
      Constant val = mypred.equatesWithConstant(fldname);
      if (val != null) {
        IndexInfo ii = entry.getValue();
        System.out.println("index on " + fldname + " used");
        return new IndexSelectPlan(myplan, ii, val);
      }
    }
    return null;
  }

  /*
   * 1. Create IndexJoinPlan if a field in the mypred (specified predicate) has an inedex.
   * 2. Add selectSubPred to SelectPlan on top of the IndexJoinPlan if exists
   * 3. Add joinSubPred to SelectPlan on top of the result plan above if exists
   * SelectPlan(SelectPlan(IndexJoinPlan + selectpred) +joinpred)
   */
  private Plan makeIndexJoin(Plan current, Schema currsch) {
    for (Entry<String, IndexInfo> entry : indexes.entrySet()) {
      String fldname = entry.getKey();
      String outerfield = mypred.equatesWithField(fldname);
      if (outerfield != null && currsch.hasField(outerfield)) {
        IndexInfo ii = entry.getValue();
        Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
        p = addSelectPred(p);
        return addJoinPred(p, currsch);
      }
    }
    return null;
  }

  private Plan makeProductJoin(Plan current, Schema currsch) {
    Plan p = makeProductPlan(current);
    return addJoinPred(p, currsch);
  }

  /*
   * Add selectpred to the given plan with SelectPlan
   */
  private Plan addSelectPred(Plan p) {
    Predicate selectpred = mypred.selectSubPred(myschema);
    if (selectpred != null)
      return new SelectPlan(p, selectpred);
    else
      return p;
  }

  /*
   * Add joinpred to the given plan with SelectPlan
   */
  private Plan addJoinPred(Plan p, Schema currsch) {
    Predicate joinpred = mypred.joinSubPred(currsch, myschema);
    if (joinpred != null)
      return new SelectPlan(p, joinpred);
    else
      return p;
  }
}
