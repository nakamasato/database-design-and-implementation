## Chapter 15: Query Optimization


1. Add two methods to `Predicate`.

    1. `selectSubPred`: return the sub-predicate that applies to the specified schema. (you can understand this statement easily with test cases)
    1. `joinSubPred`: return the sub-predicate consisting of terms that applyto the union of the two specified schemas, but not either schama separately. (you can understand this statement easily with test cases)

    ```java
    /*
     * Return the subpredicate that applies to the specified schema.
     */
    public Predicate selectSubPred(Schema sch) {
      Predicate result = new Predicate();
      for (Term t : terms)
        if (t.appliesTo(sch))
          result.terms.add(t);
      if (result.terms.isEmpty())
        return null;
      else
        return result;
    }

    /*
     * Return the subpredicate consisting of terms that apply
     * to the union of the two specified schemas,
     * but not either schema separately.
     * rhs and lhs are applied to sch1 and sch2.
     * e.g. sch1.fld1 = sch2.fld1
     */
    public Predicate joinSubPred(Schema sch1, Schema sch2) {
      Predicate result = new Predicate();
      Schema newsch = new Schema();
      newsch.addAll(sch1);
      newsch.addAll(sch2);
      for (Term t : terms)
        if (!t.appliesTo(sch1) && !t.appliesTo(sch2) && t.appliesTo(newsch))
          result.terms.add(t);
      if (result.terms.isEmpty())
        return null;
      else
        return result;
    }
    ```

1. Add `TablePlanner`

```java
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
   * Return IndexSelectPlan if possible.
   * Otherwise, return TablePlan
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
```

Add `HeuristicQueryPlanner`

```java
package simpledb.opt;

import java.util.ArrayList;
import java.util.Collection;

import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.plan.ProjectPlan;
import simpledb.plan.QueryPlanner;
import simpledb.tx.Transaction;

public class HeuristicQueryPlanner implements QueryPlanner {
  private Collection<TablePlanner> tableplanners = new ArrayList<>();
  private MetadataMgr mdm;

  public HeuristicQueryPlanner(MetadataMgr mdm) {
    this.mdm = mdm;
  }

  /*
   * Create an optimized left-deep query plan using the following heuristics:
   * 1. Choose the smallest table (considering selection predicates) to be first in the join order.
   * 2. Add the table to the join order which results in the smallest output.
   */
  @Override
  public Plan createPlan(QueryData data, Transaction tx) {

    // Step1: Create a TablePlanner object for each mentioned table
    for (String tblname : data.tables()) {
      TablePlanner tp = new TablePlanner(tblname, data.predicate(), tx, mdm);
      tableplanners.add(tp);
    }

    // Step2: Choose the lowest-size plan to begin the join order
    Plan currentplan = getLowestSelectPlan();

    // Step3: Repeatedly add a plan to the join order
    while (!tableplanners.isEmpty()) {
      Plan p = getLowestJoinPlan(currentplan);
      if (p != null)
        currentplan = p;
      else
        currentplan = getLowestProductPlan(currentplan);
    }

    // Step4: Project on the field names and return
    return new ProjectPlan(currentplan, data.fields());
  }

  private Plan getLowestSelectPlan() {
    TablePlanner besttp = null;
    Plan bestplan = null;
    for (TablePlanner tp : tableplanners) {
      Plan plan = tp.makeSelectPlan();
      if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
        besttp = tp;
        bestplan = plan;
      }
    }
    tableplanners.remove(besttp);
    return bestplan;
  }

  private Plan getLowestJoinPlan(Plan current) {
    TablePlanner besttp = null;
    Plan bestplan = null;
    for (TablePlanner tp : tableplanners) {
      Plan plan = tp.makeJoinPlan(current);
      if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
        besttp = tp;
        bestplan = plan;
      }
    }
    if (bestplan != null)
      tableplanners.remove(besttp);
    return bestplan;
  }

  private Plan getLowestProductPlan(Plan current) {
    TablePlanner besttp = null;
    Plan bestplan = null;
    for (TablePlanner tp : tableplanners) {
      Plan plan = tp.makeProductPlan(current);
      if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
        besttp = tp;
        bestplan = plan;
      }
    }
    tableplanners.remove(besttp);
    return bestplan;
  }

  public void setPlanner(Planner p) {
    // for use in planning views, which
    // for simplicity this code doesn't do.
  }
}
```
