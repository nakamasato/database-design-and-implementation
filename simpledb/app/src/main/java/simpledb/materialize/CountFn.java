package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class CountFn implements AggregationFn {
  private String fldname;
  private int count;

  public CountFn(String fldname) {
    this.fldname = fldname;
  }

  @Override
  public void processFirst(Scan s) {
    count = 1;
  }

  @Override
  public void processNext(Scan s) {
    count++;
  }

  @Override
  public String fieldName() {
    return "countof" + fldname;
  }

  @Override
  public Constant value() {
    return new Constant(count);
  }
}
