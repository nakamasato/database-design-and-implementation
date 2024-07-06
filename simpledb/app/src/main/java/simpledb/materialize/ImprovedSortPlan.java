package simpledb.materialize;

import java.util.ArrayList;
import java.util.List;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class ImprovedSortPlan implements Plan {
  private Transaction tx;
  private Plan p;
  private int blocksize;
  private Schema sch;
  private RecordComparator comp;
  private List<String> sortfields;

  public ImprovedSortPlan(Transaction tx, Plan p, List<String> sortfields, int blocksize) {
    this.tx = tx;
    this.p = p;
    this.blocksize = blocksize;
    sch = p.schema();
    this.sortfields = sortfields;
    comp = new RecordComparator(sortfields);
  }

  /*
   * Do mergesort before returning Scan
   */
  @Override
  public Scan open() {
    Scan src = p.open();
    List<TempTable> runs = splitIntoRuns(src);
    System.out.println("[SortPlan] split into " + runs.size() + " runs");
    src.close();
    while (runs.size() > 2) runs = doAMergeIteration(runs);
    System.out.println("[SortPlan] merged into " + runs.size() + " runs");
    return new SortScan(runs, comp);
  }

  @Override
  public int blockAccessed() {
    Plan mp = new MaterializePlan(tx, p);
    return mp.blockAccessed();
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
    return sch;
  }

  /*
   * split into TempTables in that all records in each TempTable
   * are sorted.
   * Example with block size 5
   * 2 6 20 4 1 16 19 3 18
   * run1: 2 6 20 4 1 -> 1 2 4 6 20
   * run2: 16 19 4 18 -> 4 16 18 19
   */
  private List<TempTable> splitIntoRuns(Scan src) {
    List<TempTable> temps = new ArrayList<>();
    src.beforeFirst();
    if (!src.next()) return temps;
    TempTable currenttemp = new TempTable(tx, sch);
    temps.add(currenttemp);
    UpdateScan currentscan = currenttemp.open();
    int recNum = 0;
    while (copy(src, currentscan)) {
      recNum++;
      if (recNum == blocksize) {
        // sort TempTable
        // sortTempTable(currentscan, currenttemp);
        // close -> write to disk
        currentscan.close();
        currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        currentscan = currenttemp.open();
        recNum = 0;
      }
    }
    currentscan.close();
    return temps;
  }

  private List<TempTable> doAMergeIteration(List<TempTable> runs) {
    List<TempTable> result = new ArrayList<>();
    while (runs.size() > 1) {
      TempTable p1 = runs.remove(0);
      TempTable p2 = runs.remove(0);
      result.add(mergeTwoRuns(p1, p2));
    }
    if (runs.size() == 1) result.add(runs.get(0));
    return result;
  }

  /*
   * Merge two runs:
   * 1. Create a new TempTable (to be merged table)
   * 2. Insert the smaller record into the destination table
   * until no more records exists.
   */
  private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
    Scan src1 = p1.open();
    Scan src2 = p2.open();
    TempTable result = new TempTable(tx, sch);
    UpdateScan dest = result.open();

    boolean hasmore1 = src1.next();
    boolean hasmore2 = src2.next();
    while (hasmore1 && hasmore2)
      if (comp.compare(src1, src2) < 0) hasmore1 = copy(src1, dest);
      else hasmore2 = copy(src2, dest);

    if (hasmore1) while (hasmore1) hasmore1 = copy(src1, dest);
    else while (hasmore2) hasmore2 = copy(src2, dest);
    src1.close();
    src2.close();
    return result;
  }

  private boolean copy(Scan src, UpdateScan dest) {
    dest.insert();
    for (String fldname : sch.fields()) dest.setVal(fldname, src.getVal(fldname));
    return src.next();
  }

  /*
   * Ref: 13.4.4. The Cost of Mergesort
   * Condition:
   * 1. The algorithm merges k runs at a time.
   * 2. There are R initial runs.
   * 3. The materialized input records require B block.
   * Split phase:
   * 1. B block accesses
   * 2. The cost of the input
   * Sort iteration: logkR iterations
   * 1. 2B block accesses for each iteration * (logkR - 1)
   */
  @Override
  public int preprocessingCost() {
    int k = 2; // merge 2 runs at once
    int r = blockAccessed() / 2; // estimated initial runs
    int splitCost = blockAccessed() + p.blockAccessed(); // cost of writing temptable + input cost
    double sortCost = 2 * blockAccessed() * (Math.log(r) / Math.log(k) - 1);
    return (int) (splitCost + sortCost);
  }

  /*
   * Sort scan
   */
  // private void sortTempTable(Scan scan) {
  // Map<String, Constant> curVals = new HashMap<>();
  // scan.beforeFirst();
  // while (scan.next()) {
  // for (String fldname : sortfields) {
  // Constant val = scan.getVal(fldname);
  // if (curVals.get(fldname) != null && curVals.get(fldname).compareTo(val)) { //
  // 前のレコードと比較して小さかったら交換する

  // }
  // curVals.put(fldname, val);
  // }
  // }
  // }
}
