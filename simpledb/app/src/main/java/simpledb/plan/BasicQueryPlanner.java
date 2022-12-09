package simpledb.plan;

import java.util.ArrayList;
import java.util.List;

import simpledb.metadata.MetadataMgr;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

/*
 * Simplest and most naive query planner
 */
public class BasicQueryPlanner implements QueryPlanner {
  private MetadataMgr mdm;

  public BasicQueryPlanner(MetadataMgr mdm) {
    this.mdm = mdm;
  }

  @Override
  public Plan createPlan(QueryData data, Transaction tx) {
    // Step 1: Create a plan for each mentioned table or view.
    List<Plan> plans = new ArrayList<>();
    for (String tblname : data.tables()) {
      String viewdef = mdm.getViewDef(tblname, tx);
      if (viewdef != null) {
        Parser parser = new Parser(viewdef);
        QueryData viewData = parser.query();
        plans.add(createPlan(viewData, tx));
      } else
        plans.add(new TablePlan(tx, tblname, mdm));
    }

    // Step 2: Create product of all table plans
    // ProductPlan(...ProductPlan(ProductPlan(p0, p1), p2, p3,...)
    // The order is arbitrary as tables() returns Collection<String>
    Plan p = plans.remove(0);
    for (Plan nextplan : plans)
      p = new ProductPlan(p, nextplan);

    // Step 3: Add a select plan for the predicate
    p = new SelectPlan(p, data.predicate());

    // Step 4: Project on the field names
    p = new ProjectPlan(p, data.fields());
    return p;
  }
}
