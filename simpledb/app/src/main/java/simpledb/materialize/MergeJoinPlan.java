package simpledb.materialize;

import java.util.Arrays;
import java.util.List;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class MergeJoinPlan implements Plan {
  private Plan p1, p2;
  private String fldname1, fldname2;
  private Schema sch = new Schema();

  public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
    this.fldname1 = fldname1;
    List<String> sortlist1 = Arrays.asList(fldname1);
    this.p1 = new SortPlan(tx, p1, sortlist1);

    this.fldname2 = fldname2;
    List<String> sortlist2 = Arrays.asList(fldname2);
    this.p2 = new SortPlan(tx, p2, sortlist2);

    sch.addAll(p1.schema());
    sch.addAll(p2.schema());
  }

  @Override
  public Scan open() {
    Scan s1 = p1.open();
    SortScan s2 = (SortScan) p2.open();
    return new MergeJoinScan(s1, s2, fldname1, fldname2);
  }

  @Override
  public int blockAccessed() {
    return p1.blockAccessed() + p2.blockAccessed();
  }

  @Override
  public int recordsOutput() {
    int maxvals = Math.max(p1.distinctValues(fldname1),
        p2.distinctValues(fldname2));
    return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
  }

  @Override
  public int distinctValues(String fldname) {
    if (p1.schema().hasField(fldname))
      return p1.distinctValues(fldname);
    else
      return p2.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return sch;
  }
}
