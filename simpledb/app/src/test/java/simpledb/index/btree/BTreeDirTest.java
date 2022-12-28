package simpledb.index.btree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

@ExtendWith(MockitoExtension.class)
public class BTreeDirTest {
  private static final String filename = "testidxdir";
  @Mock
  private Transaction tx;

  /*
   * test with level 0
   */
  @Test
  public void testBTreeDirSearchWithLevelZero() {
    BlockId blk = new BlockId(filename, 0);
    Schema sch = new Schema();
    sch.addIntField("block"); // blk num
    sch.addStringField("dataval", 10); // index on String field
    Layout layout = new Layout(sch);
    int slotsize = layout.slotSize();
    when(tx.getInt(blk, 0)).thenReturn(0); // flag: directory level
    when(tx.getInt(blk, 4)).thenReturn(2); // record num
    when(tx.getInt(blk, 12)).thenReturn(10); // leaf block num
    when(tx.getString(blk, 16)).thenReturn("test1"); // dataval = "test1"
    when(tx.getInt(blk, slotsize + 12)).thenReturn(20); // leaf block num
    when(tx.getString(blk, slotsize + 16)).thenReturn("test2"); // dataval = "tes2"

    BTreeDir bDir = new BTreeDir(tx, blk, layout);
    assertEquals(10, bDir.search(new Constant("test1")));
    assertEquals(20, bDir.search(new Constant("test2")));
  }

  /*
   * test with level 1
   */
  @Test
  public void testBTreeDirSearchWithLevelOne() {
    BlockId blk = new BlockId(filename, 0);
    BlockId blk1 = new BlockId(filename, 1);
    Schema sch = new Schema();
    sch.addIntField("block"); // blk num
    sch.addStringField("dataval", 10); // index on String field
    Layout layout = new Layout(sch);
    int slotsize = layout.slotSize();
    when(tx.getInt(blk, 0)).thenReturn(1); // flag: directory level
    when(tx.getInt(blk, 4)).thenReturn(2); // record num
    when(tx.getInt(blk, 12)).thenReturn(1); // leaf block num
    when(tx.getString(blk, 16)).thenReturn("test1"); // dataval = "test1"

    // block 1
    when(tx.getInt(blk1, 0)).thenReturn(0); // flag: directory level
    when(tx.getInt(blk1, 4)).thenReturn(2); // record num
    when(tx.getInt(blk1, 12)).thenReturn(10); // leaf block num
    when(tx.getString(blk1, 16)).thenReturn("test1"); // dataval = "test1"
    when(tx.getInt(blk1, slotsize + 12)).thenReturn(20); // leaf block num
    when(tx.getString(blk1, slotsize + 16)).thenReturn("test2"); // dataval = "tes2"

    BTreeDir bDir = new BTreeDir(tx, blk, layout);
    assertEquals(10, bDir.search(new Constant("test1")));
    assertEquals(20, bDir.search(new Constant("test2")));
  }
}
