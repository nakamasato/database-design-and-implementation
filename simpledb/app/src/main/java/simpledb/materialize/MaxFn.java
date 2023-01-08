package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MaxFn implements AggregationFn {
  private String fldname;
  private Constant val;

  public MaxFn(String fldname) {
    this.fldname = fldname;
  }

  @Override
  public void processFirst(Scan s) {
    val = s.getVal(fldname);
  }

  @Override
  public void processNext(Scan s) {
    Constant newval = s.getVal(fldname);
    if (val.compareTo(newval) > 0)
      val = newval;
  }

  @Override
  public String fieldName() {
    return "maxof" + fldname;
  }

  @Override
  public Constant value() {
    return val;
  }
}
