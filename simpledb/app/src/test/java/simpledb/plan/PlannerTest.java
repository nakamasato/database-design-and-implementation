package simpledb.plan;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.parse.CreateIndexData;
import simpledb.parse.CreateTableData;
import simpledb.parse.CreateViewData;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.ModifyData;
import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

@ExtendWith(MockitoExtension.class)
public class PlannerTest {
  @Mock
  QueryPlanner qplanner;
  @Mock
  UpdatePlanner uplanner;
  @Mock
  Transaction tx;

  @Test
  public void testCreateQueryPlan() {
    Planner planner = new Planner(qplanner, uplanner);
    planner.createQueryPlan("select fld1 from tbl1", tx);
    verify(qplanner).createPlan(any(QueryData.class), any(Transaction.class));
  }

  @Test
  public void testExecuteInsert() {
    Planner planner = new Planner(qplanner, uplanner);
    planner.executeUpdate("insert into tbl1(fld1) values (1)", tx);
    verify(uplanner).executeInsert(any(InsertData.class), any(Transaction.class));
  }

  @Test
  public void testExecuteDelete() {
    Planner planner = new Planner(qplanner, uplanner);
    planner.executeUpdate("delete from tbl1 where fld1 = 1", tx);
    verify(uplanner).executeDelete(any(DeleteData.class), any(Transaction.class));
  }

  @Test
  public void testExecuteModify() {
    Planner planner = new Planner(qplanner, uplanner);
    planner.executeUpdate("update tbl1 set fld1 = 1", tx);
    verify(uplanner).executeModify(any(ModifyData.class), any(Transaction.class));
  }

  @Test
  public void testExecuteCreateTable() {
    Planner planner = new Planner(qplanner, uplanner);
    planner.executeUpdate("create table tbl1 (fld1 int, fld2 varchar(10))", tx);
    verify(uplanner).executeCreateTable(any(CreateTableData.class), any(Transaction.class));
  }

  @Test
  public void testExecuteCreateView() {
    Planner planner = new Planner(qplanner, uplanner);
    planner.executeUpdate("create view testview as select fld1 from tbl1", tx);
    verify(uplanner).executeCreateView(any(CreateViewData.class), any(Transaction.class));
  }

  @Test
  public void testExecuteCreateIndex() {
    Planner planner = new Planner(qplanner, uplanner);
    planner.executeUpdate("create index test_idx on tbl1(fld1)", tx);
    verify(uplanner).executeCreateIndex(any(CreateIndexData.class), any(Transaction.class));
  }
}
