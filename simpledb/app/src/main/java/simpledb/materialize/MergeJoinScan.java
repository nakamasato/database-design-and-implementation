package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MergeJoinScan implements Scan {
  private Scan s1;
  private SortScan s2;
  private String fldname1;
  private String fldname2;
  private Constant joinval = null;

  public MergeJoinScan(Scan s1, SortScan s2, String fldname1, String fldname2) {
    this.s1 = s1;
    this.s2 = s2;
    this.fldname1 = fldname1;
    this.fldname2 = fldname2;
    beforeFirst();
  }

  @Override
  public void beforeFirst() {
    s1.beforeFirst();
    s2.beforeFirst();
  }

  /*
   * 1. If the next RHS record has the same join value, then move it.
   * 2. If the next LHS record has the same join value, move the RHS scan
   * back to the first record having that join value.
   * 3. Otherwise, repeatedly move the scan having the smallest value until
   * a common join value is found.
   * 4. If there's no more records in both RHS and LHS, return false.
   */
  @Override
  public boolean next() {
    boolean hasmore2 = s2.next();
    if (hasmore2 && s2.getVal(fldname2).equals(joinval)) {
      System.out.println("[MergeJoinScan] next increments RHS joinval: " + joinval);
      return true;
    }

    boolean hasmore1 = s1.next();
    if (hasmore1 && s1.getVal(fldname1).equals(joinval)) {
      s2.restorePosition();
      System.out.println(
          "[MergeJoinScan] next increments LHS and move RHS back to the starting point of joinval: " + joinval);
      return true;
    }

    while (hasmore1 && hasmore2) {
      Constant v1 = s1.getVal(fldname1);
      Constant v2 = s2.getVal(fldname2);
      if (v1.compareTo(v2) < 0)
        hasmore1 = s1.next();
      else if (v1.compareTo(v2) > 0)
        hasmore2 = s2.next();
      else {
        s2.savePosition();
        joinval = s2.getVal(fldname2);
        System.out.println("[MergeJoinScan] next update joinval: " + joinval);
        return true;
      }
    }
    System.out.println("[MergeJoinScan] next no more next: " + joinval);
    return false;
  }

  @Override
  public int getInt(String fldname) {
    if (s1.hasField(fldname))
      return s1.getInt(fldname);
    else
      return s2.getInt(fldname);
  }

  @Override
  public String getString(String fldname) {
    if (s1.hasField(fldname))
      return s1.getString(fldname);
    else
      return s2.getString(fldname);
  }

  @Override
  public Constant getVal(String fldname) {
    if (s1.hasField(fldname))
      return s1.getVal(fldname);
    else
      return s2.getVal(fldname);
  }

  @Override
  public boolean hasField(String fldname) {
    return s1.hasField(fldname) || s2.hasField(fldname);
  }

  @Override
  public void close() {
    s1.close();
    s2.close();
  }
}
