package simpledb.materialize;

import java.util.Arrays;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

/*
 * Create a sort scan, given a list of 1 or 2 runs.
 * If there is only 1 run, then s2 will be null and
 * hasmore2 will be false.
 */
public class SortScan implements Scan {
  private UpdateScan s1;
  private UpdateScan s2 = null;
  private UpdateScan currentscan = null;
  private RecordComparator comp;
  private boolean hasmore1;
  private boolean hasmore2 = false;
  private List<RID> savedposition;

  public SortScan(List<TempTable> runs, RecordComparator comp) {
    this.comp = comp;
    if (!runs.isEmpty()) {
      s1 = runs.get(0).open();
      hasmore1 = s1.next();
    }
    if (runs.size() > 1) {
      s2 = runs.get(1).open();
      hasmore2 = s2.next();
    }
  }

  /*
   * Position the scan before the first record in sorted order.
   * Internally, it moves to the first record of each underlying scan.
   * The variable currentscan is set to null, indicating that there is
   * no current scan.
   */
  @Override
  public void beforeFirst() {
    currentscan = null;
    s1.beforeFirst();
    hasmore1 = s1.next();
    if (s2 != null) {
      s2.beforeFirst();
      hasmore2 = s2.next();
    }
  }

  /*
   * Increment currentscan
   * Set currentscan after comparing s1 and s2
   */
  @Override
  public boolean next() {
    if (currentscan != null) {
      if (currentscan == s1)
        hasmore1 = s1.next();
      else if (currentscan == s2)
        hasmore2 = s2.next();
    }

    if (!hasmore1 && !hasmore2) // false & false
      return false;
    else if (hasmore1 && hasmore2) { // true & true
      if (comp.compare(s1, s2) < 0)
        currentscan = s1;
      else
        currentscan = s2;
    } else if (hasmore1) // true & false
      currentscan = s1;
    else // false & true
      currentscan = s2;
    return true;
  }

  @Override
  public int getInt(String fldname) {
    return currentscan.getInt(fldname);
  }

  @Override
  public String getString(String fldname) {
    return currentscan.getString(fldname);
  }

  @Override
  public Constant getVal(String fldname) {
    return currentscan.getVal(fldname);
  }

  @Override
  public boolean hasField(String fldname) {
    return currentscan.hasField(fldname);
  }

  @Override
  public void close() {
    if (s1 != null)
      s1.close();
    if (s2 != null)
      s2.close();
  }

  public void restorePosition() {
    RID rid1 = savedposition.get(0);
    RID rid2 = savedposition.get(1);
    s1.moveToRid(rid1);
    if (rid2 != null)
      s2.moveToRid(rid2);
  }

  public void savePosition() {
    RID rid1 = s1.getRid();
    RID rid2 = (s2 == null) ? null : s2.getRid();
    savedposition = Arrays.asList(rid1, rid2);
  }
}
