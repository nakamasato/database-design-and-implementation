package simpledb.plan;

import simpledb.parse.CreateIndexData;
import simpledb.parse.CreateTableData;
import simpledb.parse.CreateViewData;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.ModifyData;
import simpledb.tx.Transaction;

/*
 * Interface implemented by planners for SQL
 * insert, delete, and modify statement
 */
public interface UpdatePlanner {
  public int executeInsert(InsertData data, Transaction tx);

  public int executeDelete(DeleteData data, Transaction tx);

  public int executeModify(ModifyData data, Transaction tx);

  public int executeCreateTable(CreateTableData data, Transaction tx);

  public int executeCreateView(CreateViewData data, Transaction tx);

  public int executeCreateIndex(CreateIndexData data, Transaction tx);
}
