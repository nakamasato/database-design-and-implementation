package simpledb.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.Test;

import simpledb.buffer.BufferMgr;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class TableMgrTest {
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
    // 4 bytes for the empty/inuse flag, 54 bytes for name string (4 bytes for the byte
    // length and 50 bytes for bytes themselves)
    assertEquals(4 + 54, layout.offset("count"));
  }
}
