package simpledb.plan;

import java.util.List;

import simpledb.query.ProjectScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

/*
 * Plan class corresponding to the project
 * relational algebra operator
 */
public class ProjectPlan implements Plan {
  private Plan p;
  private Schema schema = new Schema();

  public ProjectPlan(Plan p, List<String> fieldlist) {
    this.p = p;
    for (String fldname : fieldlist)
      schema.add(fldname, p.schema());
  }

  @Override
  public Scan open() {
    Scan s = p.open();
    return new ProjectScan(s, schema.fields());
  }

  @Override
  public int blockAccessed() {
    return p.blockAccessed();
  }

  @Override
  public int recordsOutput() {
    return p.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    return p.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public int preprocessingCost() {
    return 0;
  }
}
