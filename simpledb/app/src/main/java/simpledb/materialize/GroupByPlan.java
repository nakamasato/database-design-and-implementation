package simpledb.materialize;

import java.util.List;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class GroupByPlan implements Plan {
  private Plan p;
  private List<String> groupfields;
  private List<AggregationFn> aggfns;
  private Schema sch = new Schema(); // contains groupfields & aggregation fields

  public GroupByPlan(Transaction tx, Plan p, List<String> groupfields, List<AggregationFn> aggfns) {
    this.p = new SortPlan(tx, p, groupfields);
    this.groupfields = groupfields;
    this.aggfns = aggfns;
    for (String fldname : groupfields)
      sch.add(fldname, p.schema());
    for (AggregationFn fn : aggfns)
      sch.addIntField(fn.fieldName());
  }

  @Override
  public Scan open() {
    Scan s = p.open();
    return new GroupByScan(s, groupfields, aggfns);
  }

  @Override
  public int blockAccessed() {
    return p.blockAccessed();
  }

  @Override
  public int recordsOutput() {
    int numgroups = 1;
    for (String fldname : groupfields)
      numgroups += p.distinctValues(fldname);
    return numgroups;
  }

  @Override
  public int distinctValues(String fldname) {
    if (p.schema().hasField(fldname))
      return p.distinctValues(fldname);
    else
      return recordsOutput();
  }

  /*
   * Return the schema of the output table.
   * The schema consists of the group fields and
   * aggregation result fields.
   */
  @Override
  public Schema schema() {
    return sch;
  }

  /*
   * No cost in preprocessing
   */
  @Override
  public int preprocessingCost() {
    return 0;
  }
}
