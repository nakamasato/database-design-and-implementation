package simpledb.plan;

import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

/*
 * Interface implemented by planners for
 * the SQL select statement.
 */
public interface QueryPlanner {
  public Plan createPlan(QueryData data, Transaction tx);
}
