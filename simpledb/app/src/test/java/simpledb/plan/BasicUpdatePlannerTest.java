package simpledb.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.file.BlockId;
import simpledb.metadata.MetadataMgr;
import simpledb.metadata.StatInfo;
import simpledb.parse.CreateIndexData;
import simpledb.parse.CreateTableData;
import simpledb.parse.CreateViewData;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.ModifyData;
import simpledb.parse.QueryData;
import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Layout;
import simpledb.record.RecordPage;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

@ExtendWith(MockitoExtension.class)
public class BasicUpdatePlannerTest {
  @Mock
  private MetadataMgr mdm;

  @Mock
  private Transaction tx;

  /*
   * SQL: delete from tbl1 where fld1 = 1;
   * Record: 1, 1, 3, 3, 1, 3
   */
  @Test
  public void testDeleteData() {
    int numBlocks = 2;
    int numRecs = 6;
    int blockSize = 32; // 4 slots per block
    Schema sch = new Schema();
    sch.addIntField("fld1");
    Layout layout = new Layout(sch);
    int slotsize = layout.slotSize(); // slotsize = 8 (4 for flag, 4 for an int field)
    assertEquals(8, slotsize);
    StatInfo si = new StatInfo(numBlocks, numRecs); // 4, 2 slots in each block

    when(mdm.getLayout("tbl1", tx)).thenReturn(layout);
    when(mdm.getStatInfo("tbl1", layout, tx)).thenReturn(si);
    when(tx.size("tbl1.tbl")).thenReturn(numBlocks); // 2 blocks in tbl1.tbl
    when(tx.blockSize()).thenReturn(blockSize);

    // flag and value for each record
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 0
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 4)).thenReturn(1); // value for record 0
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 8)).thenReturn(RecordPage.USED); // flag for record 1
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 12)).thenReturn(1); // value for record 1
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 16)).thenReturn(RecordPage.USED); // flag for record 2
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 20)).thenReturn(3); // value for record 2
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 24)).thenReturn(RecordPage.USED); // flag for record 3
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 28)).thenReturn(3); // value for record 3
    when(tx.getInt(new BlockId("tbl1.tbl", 1), 0)).thenReturn(RecordPage.USED); // flag for record 4
    when(tx.getInt(new BlockId("tbl1.tbl", 1), 4)).thenReturn(1); // value for record 4
    when(tx.getInt(new BlockId("tbl1.tbl", 1), 8)).thenReturn(RecordPage.USED); // flag for record 5
    when(tx.getInt(new BlockId("tbl1.tbl", 1), 12)).thenReturn(3); // value for record 5

    BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
    Term term = new Term(new Expression("fld1"), new Expression(new Constant(1))); // fld1 = 1
    Predicate pred = new Predicate(term);
    DeleteData data = new DeleteData("tbl1", pred);
    int count = basicUpdatePlanner.executeDelete(data, tx);
    assertEquals(3, count);

    // Flag of record 0~2 are set to empty
    verify(tx).setInt(new BlockId("tbl1.tbl", 0), 0, RecordPage.EMPTY, true);
    verify(tx).setInt(new BlockId("tbl1.tbl", 0), 8, RecordPage.EMPTY, true);
    verify(tx).setInt(new BlockId("tbl1.tbl", 1), 0, RecordPage.EMPTY, true);
  }

  /*
   * SQL: update tbl1 set fld1 = 1000 where fld1 = 1;
   * Record: 1, 1, 3, 3, 1, 3
   */
  @Test
  public void testModifyData() {
    int numBlocks = 2;
    int numRecs = 6;
    int blockSize = 32; // 4 slots per block
    int newVal = 1000;
    Schema sch = new Schema();
    sch.addIntField("fld1");
    Layout layout = new Layout(sch);
    int slotsize = layout.slotSize(); // slotsize = 8 (4 for flag, 4 for an int field)
    assertEquals(8, slotsize);

    StatInfo si = new StatInfo(numBlocks, numRecs); // 4, 2 slots in each block
    when(mdm.getLayout("tbl1", tx)).thenReturn(layout);
    when(mdm.getStatInfo("tbl1", layout, tx)).thenReturn(si);
    when(tx.size("tbl1.tbl")).thenReturn(numBlocks); // 2 blocks in tbl1.tbl
    when(tx.blockSize()).thenReturn(blockSize);

    // flag and value for each record
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 0
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 4)).thenReturn(1); // value for record 0
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 8)).thenReturn(RecordPage.USED); // flag for record 1
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 12)).thenReturn(1); // value for record 1
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 16)).thenReturn(RecordPage.USED); // flag for record 2
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 20)).thenReturn(3); // value for record 2
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 24)).thenReturn(RecordPage.USED); // flag for record 3
    when(tx.getInt(new BlockId("tbl1.tbl", 0), 28)).thenReturn(3); // value for record 3
    when(tx.getInt(new BlockId("tbl1.tbl", 1), 0)).thenReturn(RecordPage.USED); // flag for record 4
    when(tx.getInt(new BlockId("tbl1.tbl", 1), 4)).thenReturn(1); // value for record 4
    when(tx.getInt(new BlockId("tbl1.tbl", 1), 8)).thenReturn(RecordPage.USED); // flag for record 5
    when(tx.getInt(new BlockId("tbl1.tbl", 1), 12)).thenReturn(3); // value for record 5

    BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
    Term term = new Term(new Expression("fld1"), new Expression(new Constant(1))); // fld1 = 1
    Predicate pred = new Predicate(term);
    ModifyData data = new ModifyData("tbl1", "fld1", new Expression(new Constant(newVal)), pred);
    int count = basicUpdatePlanner.executeModify(data, tx);
    assertEquals(3, count);

    // record 0~2 are updated with newVal
    verify(tx).setInt(new BlockId("tbl1.tbl", 0), 4, newVal, true);
    verify(tx).setInt(new BlockId("tbl1.tbl", 0), 12, newVal, true);
    verify(tx).setInt(new BlockId("tbl1.tbl", 1), 4, newVal, true);
  }

  /*
   * SQL: insert into tbl1 values (1, 'test');
   */
  @Test
  public void testInsertData() {
    int blockSize = 32;
    Schema sch = new Schema();
    sch.addIntField("fld1");
    sch.addStringField("fld2", 10);
    Layout layout = new Layout(sch);
    int slotsize = layout.slotSize(); // slotsize = 22 (4 for flag, 4 for an int field, 4 + 10 for a string field)
    assertEquals(22, slotsize);

    StatInfo si = new StatInfo(0, 0); // 4, 2 slots in each block
    when(mdm.getLayout("tbl1", tx)).thenReturn(layout);
    when(mdm.getStatInfo("tbl1", layout, tx)).thenReturn(si);
    when(tx.size("tbl1.tbl")).thenReturn(1); // 1 block in tbl1.tbl
    when(tx.blockSize()).thenReturn(blockSize);

    BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
    InsertData data = new InsertData("tbl1", Arrays.asList("fld1", "fld2"),
        Arrays.asList(new Constant(1), new Constant("test")));
    basicUpdatePlanner.executeInsert(data, tx);

    // record (1, 'test') is inserted
    verify(tx).setInt(new BlockId("tbl1.tbl", 0), 0, RecordPage.USED, true);
    verify(tx).setInt(new BlockId("tbl1.tbl", 0), 4, 1, true);
    verify(tx).setString(new BlockId("tbl1.tbl", 0), 8, "test", true);
  }

  /*
   * SQL: create table tbl1 (fld1 int, fld2 varchar(10))
   */
  @Test
  public void testCreateTable() {
    Schema sch = new Schema();
    sch.addIntField("fld1");
    sch.addStringField("fld2", 10);

    BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
    CreateTableData data = new CreateTableData("tbl1", sch);
    basicUpdatePlanner.executeCreateTable(data, tx);

    verify(mdm).createTable("tbl1", sch, tx);
  }

  /*
   * SQL: create view viewname as select fld1 from tbl1 where fld1 = 1;
   */
  @Test
  public void testCreateView() {
    BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
    Term term = new Term(new Expression("fld1"), new Expression(new Constant(1)));
    Predicate pred = new Predicate(term);
    QueryData queryData = new QueryData(Arrays.asList("fld1"), Arrays.asList("tbl1"), pred);
    CreateViewData data = new CreateViewData("viewname", queryData);
    basicUpdatePlanner.executeCreateView(data, tx);

    verify(mdm).createView("viewname", data.viewDef(), tx);
  }

  /*
   * SQL: create index test_idx on tbl1(fld1)
   */
  @Test
  public void testCreateIndex() {
    BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
    CreateIndexData data = new CreateIndexData("test_idx", "tbl1", "fld1");
    basicUpdatePlanner.executeCreateIndex(data, tx);

    verify(mdm).createIndex("test_idx", "tbl1", "fld1", tx);
  }
}
