package simpledb.materialize;

import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class GroupByScan implements Scan {
  private Scan s;
  private List<String> groupfields;
  private List<AggregationFn> aggfns;
  private GroupValue groupval;
  private boolean moregroups; // boolean to indicates if the underlying scan has records to read

  public GroupByScan(Scan s, List<String> groupfields, List<AggregationFn> aggfns) {
    this.s = s;
    this.groupfields = groupfields;
    this.aggfns = aggfns;
    beforeFirst();
  }

  @Override
  public void beforeFirst() {
    s.beforeFirst();
    moregroups = s.next();
  }

  /*
   * read until a new group value appears.
   * moregroups is always true until the underlying scan finishes scanning
   */
  @Override
  public boolean next() {
    if (!moregroups)
      return false;
    for (AggregationFn fn : aggfns)
      fn.processFirst(s);
    groupval = new GroupValue(s, groupfields);
    while (moregroups = s.next()) {
      GroupValue gv = new GroupValue(s, groupfields);
      if (!groupval.equals(gv))
        break;
      for (AggregationFn fn : aggfns)
        fn.processNext(s);
    }
    return true;
  }

  @Override
  public int getInt(String fldname) {
    return getVal(fldname).asInt();
  }

  @Override
  public String getString(String fldname) {
    return getVal(fldname).asString();
  }

  @Override
  public Constant getVal(String fldname) {
    if (groupfields.contains(fldname))
      return groupval.getVal(fldname);
    for (AggregationFn fn : aggfns)
      if (fn.fieldName().equals(fldname))
        return fn.value();
    throw new RuntimeException("field: " + fldname + " not found.");
  }

  @Override
  public boolean hasField(String fldname) {
    if (groupfields.contains(fldname))
      return true;
    for (AggregationFn fn : aggfns)
      if (fn.fieldName().equals(fldname))
        return true;
    return false;
  }

  @Override
  public void close() {
    s.close();
  }
}
