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
