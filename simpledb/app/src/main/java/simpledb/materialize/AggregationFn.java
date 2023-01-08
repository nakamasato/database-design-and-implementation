package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/*
 * Aggregation function used by groupby operator
 */
public interface AggregationFn {
  void processFirst(Scan s);

  void processNext(Scan s);

  String fieldName();

  Constant value();
}
