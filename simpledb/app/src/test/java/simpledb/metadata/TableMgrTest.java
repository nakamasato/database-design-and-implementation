package simpledb.metadata;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.record.Layout;
import simpledb.record.RecordPage;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

@ExtendWith(MockitoExtension.class)
public class TableMgrTest {
  @Mock
  private Transaction tx;

  @Test
  public void testTblCatLayout() throws Exception {
    when(tx.getInt(new BlockId("tblcat.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 0
    when(tx.getString(new BlockId("tblcat.tbl", 0), 4)).thenReturn("tblcat"); // value for record 0
    when(tx.getInt(new BlockId("tblcat.tbl", 0), 24)).thenReturn(28); // flag for record 1
    when(tx.size("tblcat.tbl")).thenReturn(10); // 10 blocks in tbl1.tbl

    when(tx.getInt(new BlockId("fldcat.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 0
    when(tx.getString(new BlockId("fldcat.tbl", 0), 4)).thenReturn("tblcat"); // tblname
    when(tx.getString(new BlockId("fldcat.tbl", 0), 24)).thenReturn("tblname"); // fldname
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 44)).thenReturn(VARCHAR); // type
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 48)).thenReturn(16); // length
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 52)).thenReturn(4); // offset
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 56)).thenReturn(RecordPage.USED); // flag for record 0
    when(tx.getString(new BlockId("fldcat.tbl", 0), 56 + 4)).thenReturn("tblcat"); // tblname
    when(tx.getString(new BlockId("fldcat.tbl", 0), 56 + 24)).thenReturn("slotsize"); // fldname
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 44)).thenReturn(INTEGER); // type
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 48)).thenReturn(0); // length
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 52)).thenReturn(24); // offset
    when(tx.size("fldcat.tbl")).thenReturn(10); // 10 blocks in tbl1.tbl
    when(tx.blockSize()).thenReturn(400);
    TableMgr tableMgr = new TableMgr(false, tx);

    Layout layout = tableMgr.getLayout(TableMgr.TBL_CAT_TABLE, tx);
    assertEquals(4, layout.offset(TableMgr.TBL_CAT_FIELD_TABLE_NAME));
    assertEquals(24, layout.offset(TableMgr.TBL_CAT_FIELD_SLOTSIZE));
    assertEquals(28, layout.slotSize());
  }

  @Test
  public void testFldCatLayout() throws Exception {
    // tblcat.tbl
    when(tx.getInt(new BlockId("tblcat.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 1
    when(tx.getString(new BlockId("tblcat.tbl", 0), 4)).thenReturn("fldcat"); // tblname for record 1
    when(tx.getInt(new BlockId("tblcat.tbl", 0), 24)).thenReturn(56); // fldcat's slotsize
    when(tx.size("tblcat.tbl")).thenReturn(10); // 10 blocks in tbl1.tbl

    // fldcat.tbl
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for fldcat.tblname
    when(tx.getString(new BlockId("fldcat.tbl", 0), 4)).thenReturn("fldcat"); // tblname
    when(tx.getString(new BlockId("fldcat.tbl", 0), 24)).thenReturn("tblname"); // fldname
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 44)).thenReturn(VARCHAR); // type
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 48)).thenReturn(16); // length
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 52)).thenReturn(4); // offset
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 56)).thenReturn(RecordPage.USED); // flag for fldcat.fldname
    when(tx.getString(new BlockId("fldcat.tbl", 0), 56 + 4)).thenReturn("fldcat"); // tblname
    when(tx.getString(new BlockId("fldcat.tbl", 0), 56 + 24)).thenReturn("fldname"); // fldname
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 44)).thenReturn(VARCHAR); // type
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 48)).thenReturn(16); // length
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 52)).thenReturn(24); // offset
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 112)).thenReturn(RecordPage.USED); // flag for fldcat.type
    when(tx.getString(new BlockId("fldcat.tbl", 0), 112 + 4)).thenReturn("fldcat"); // tblname
    when(tx.getString(new BlockId("fldcat.tbl", 0), 112 + 24)).thenReturn("type"); // fldname
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 112 + 44)).thenReturn(INTEGER); // type
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 112 + 48)).thenReturn(0); // length
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 112 + 52)).thenReturn(44); // offset
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 168)).thenReturn(RecordPage.USED); // flag for fldcat.length
    when(tx.getString(new BlockId("fldcat.tbl", 0), 168 + 4)).thenReturn("fldcat"); // tblname
    when(tx.getString(new BlockId("fldcat.tbl", 0), 168 + 24)).thenReturn("length"); // fldname
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 168 + 44)).thenReturn(INTEGER); // type
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 168 + 48)).thenReturn(0); // length
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 168 + 52)).thenReturn(48); // offset
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 224)).thenReturn(RecordPage.USED); // flag for fldcat.offset
    when(tx.getString(new BlockId("fldcat.tbl", 0), 224 + 4)).thenReturn("fldcat"); // tblname
    when(tx.getString(new BlockId("fldcat.tbl", 0), 224 + 24)).thenReturn("offset"); // fldname
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 224 + 44)).thenReturn(INTEGER); // type
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 224 + 48)).thenReturn(0); // length
    when(tx.getInt(new BlockId("fldcat.tbl", 0), 224 + 52)).thenReturn(52); // offset
    when(tx.size("fldcat.tbl")).thenReturn(10); // 10 blocks in tbl1.tbl
    when(tx.blockSize()).thenReturn(400);
    TableMgr tableMgr = new TableMgr(false, tx);

    Layout layout = tableMgr.getLayout(TableMgr.FLD_CAT_TABLE, tx);
    assertEquals(4, layout.offset(TableMgr.FLD_CAT_FIELD_TABLE_NAME));
    assertEquals(24, layout.offset(TableMgr.FLD_CAT_FIELD_FEILD_NAME));
    assertEquals(44, layout.offset(TableMgr.FLD_CAT_FIELD_TYPE));
    assertEquals(48, layout.offset(TableMgr.FLD_CAT_FIELD_LENGTH));
    assertEquals(52, layout.offset(TableMgr.FLD_CAT_FIELD_OFFSET));
    assertEquals(56, layout.slotSize());
  }

  @Test
  public void testCreateTableGetLayout() throws Exception {
    File dbDirectory = new File("datadir");
    FileMgr fm = new FileMgr(dbDirectory, 400);
    LogMgr lm = new LogMgr(fm, "simpledb.log");
    BufferMgr bm = new BufferMgr(fm, lm, 8);
    Transaction tx = new Transaction(fm, lm, bm);
    TableMgr tableMgr = new TableMgr(true, tx);

    Schema sch = new Schema();
    sch.addStringField("name", 50);
    sch.addIntField("count");
    tableMgr.createTable("test_table", sch, tx);

    Layout layout = tableMgr.getLayout("test_table", tx); // read via TableScan (from the file)

    assertEquals(2, layout.schema().fields().size());
    assertEquals(4, layout.offset("name"));
    // 4 bytes for the empty/inuse flag, 54 bytes for name string (4 bytes for the
    // byte
    // length and 50 bytes for bytes themselves)
    assertEquals(4 + 54, layout.offset("count"));
  }
}
