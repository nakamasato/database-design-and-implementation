package simpledb.plan;

import simpledb.parse.CreateIndexData;
import simpledb.parse.CreateTableData;
import simpledb.parse.CreateViewData;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.ModifyData;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

public class Planner {
  private QueryPlanner qplanner;
  private UpdatePlanner uplanner;

  public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
    this.qplanner = qplanner;
    this.uplanner = uplanner;
  }

  public Plan createQueryPlan(String qry, Transaction tx) {
    Parser parser = new Parser(qry);
    QueryData data = parser.query();
    verifyQuery(data);
    return qplanner.createPlan(data, tx);
  }

  public int executeUpdate(String cmd, Transaction tx) {
    Parser parser = new Parser(cmd);
    Object data = parser.updateCmd();
    verifyUpdate(data);
    if (data instanceof InsertData)
      return uplanner.executeInsert((InsertData) data, tx);
    else if (data instanceof DeleteData)
      return uplanner.executeDelete((DeleteData) data, tx);
    else if (data instanceof ModifyData)
      return uplanner.executeModify((ModifyData) data, tx);
    else if (data instanceof CreateTableData)
      return uplanner.executeCreateTable((CreateTableData) data, tx);
    else if (data instanceof CreateViewData)
      return uplanner.executeCreateView((CreateViewData) data, tx);
    else if (data instanceof CreateIndexData)
      return uplanner.executeCreateIndex((CreateIndexData) data, tx);
    else
      return 0;
  }

  private void verifyQuery(QueryData data) {
    // TODO
  }

  private void verifyUpdate(Object data) {
    // TODO
  }
}
