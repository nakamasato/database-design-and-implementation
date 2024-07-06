package simpledb.multibuffer;

import java.util.ArrayList;
import java.util.List;
import simpledb.file.BlockId;
import simpledb.materialize.MaterializePlan;
import simpledb.materialize.RecordComparator;
import simpledb.materialize.SortScan;
import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/*
 * Implement Multibuffer Sorting:
 * 1. Split phase: Repeat until there are no more input records:
 *   1. Pin k buffers, and read k blocks of input records into them.
 *   2. Use an internal sorting algorithm to sort these records.
 *   3. Write the contents of the buffers to a temporary table.
 *   4. Unpin the buffers.
 *   5. Add the temporary table to the run-list.
 * 2. Merge phase: Repeat until the run-list contains one temporary table
 *   1. Repeat until the run-list is empty.
 *     1. Open scans for k of the temporary tables.
 *     2. Open a scan for a new temporary table.
 *     3. Merge the k scans into the new one.
 *     4. Add the new temporary table to list L.
 *   2. Add the contents of L to the run-list.
 */
public class MultibufferSortPlan implements Plan {
  private Transaction tx;
  private Plan p;
  private Schema sch;
  private RecordComparator comp;

  public MultibufferSortPlan(Transaction tx, Plan p, List<String> sortfields) {
    this.tx = tx;
    this.p = p;
    sch = p.schema();
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
   * split into k TempTables in that all records in each TempTable
   * are sorted.
   */
  private List<TempTable> splitIntoRuns(Scan src) {
    // get the number of available buffers
    int size = blockAccessed();
    int available = tx.availableBuffs();
    int numbuffs = BufferNeeds.bestRoot(available, size);

    // read k blocks, sort them and write to temporary tables.
    List<TempTable> temps = new ArrayList<>();
    while (src.next()) {
      for (int i = 0; i < numbuffs; i++) {
        // pin buffer
        // read record to buffer
        BlockId blk = new BlockId(null, i);
        tx.pin(blk);
      }
    }

    // existing logic:
    // 1. Create TempTable and add it to temps
    // 2. Copy the source record to TempTablescan until all the records in the source are
    // copied.
    //     but keep all records in a TempTable are sorted. -> split if smaller item appears.
    // List<TempTable> temps = new ArrayList<>();
    src.beforeFirst();
    if (!src.next()) return temps;
    TempTable currenttemp = new TempTable(tx, sch);
    temps.add(currenttemp);
    UpdateScan currentscan = currenttemp.open();
    while (copy(src, currentscan)) // copy to temporary table one by one until
    if (comp.compare(src, currentscan) < 0) {
        currentscan.close();
        currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        currentscan = currenttemp.open();
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

  /*
   * Set the source value to the destination for each field
   */
  private boolean copy(Scan src, UpdateScan dest) {
    dest.insert();
    for (String fldname : sch.fields()) dest.setVal(fldname, src.getVal(fldname));
    return src.next();
  }

  @Override
  public int preprocessingCost() {
    return 0;
  }
}
