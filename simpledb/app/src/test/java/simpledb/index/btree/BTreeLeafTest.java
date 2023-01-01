package simpledb.index.btree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

@ExtendWith(MockitoExtension.class)
public class BTreeLeafTest {
  private static final String filename = "testidxleaf";
  @Mock
  private Transaction tx;

  /*
   * Insert an index leaf record without splitting
   */
  @Test
  public void testInsertBTreeLeafWithoutSplit() {
    BlockId blk = new BlockId(filename, 0);
    Schema sch = new Schema();
    sch.addIntField("block"); // int field
    sch.addStringField("dataval", 10); // string field
    sch.addIntField("id"); // rid
    Layout layout = new Layout(sch);
    int slotsize = layout.slotSize();
    when(tx.getInt(blk, 0)).thenReturn(-1); // flag
    when(tx.getInt(blk, 4)).thenReturn(0); // record num
    when(tx.blockSize()).thenReturn(slotsize * 2);

    BTreeLeaf bLeaf = new BTreeLeaf(tx, blk, layout, new Constant("test1"));
    DirEntry e = bLeaf.insert(new RID(0, 0));
    assertNull(e);
  }

  /*
   * test getDataRid() + next() with no overflow block
   */
  @Test
  public void testBTreeLeafWithoutOverflow() {
    BlockId blk = new BlockId(filename, 0);
    Schema sch = new Schema();
    sch.addIntField("block"); // int field
    sch.addStringField("dataval", 10); // string field
    sch.addIntField("id"); // rid
    Layout layout = new Layout(sch);
    int slotsize = layout.slotSize();
    when(tx.getInt(blk, 0)).thenReturn(-1); // flag
    when(tx.getInt(blk, 4)).thenReturn(2); // record num
    when(tx.getInt(blk, 12)).thenReturn(10); // record block num
    when(tx.getString(blk, 16)).thenReturn("test1"); // dataval
    when(tx.getInt(blk, 30)).thenReturn(102); // record slot id
    when(tx.getInt(blk, slotsize + 12)).thenReturn(20); // leaf block num
    when(tx.getString(blk, slotsize + 16)).thenReturn("test1"); // dataval
    when(tx.getInt(blk, slotsize + 30)).thenReturn(205); // record slot id

    BTreeLeaf bLeaf = new BTreeLeaf(tx, blk, layout, new Constant("test1"));
    assertEquals(new RID(10, 102), bLeaf.getDataRid());
    assertTrue(bLeaf.next());
    assertEquals(new RID(20, 205), bLeaf.getDataRid());
    assertFalse(bLeaf.next());
  }

  /*
   * test getDataRid() + next() with an overflow block
   */
  @Test
  public void testBTreeLeafWithOverflow() {
    BlockId blk = new BlockId(filename, 0);
    BlockId overBlk = new BlockId(filename, 10);
    Schema sch = new Schema();
    sch.addIntField("block"); // int field
    sch.addStringField("dataval", 10); // string field
    sch.addIntField("id"); // rid
    Layout layout = new Layout(sch);
    int slotsize = layout.slotSize();
    when(tx.getInt(blk, 0)).thenReturn(10); // overflow block
    when(tx.getInt(blk, 4)).thenReturn(2); // record num
    when(tx.getInt(blk, 12)).thenReturn(10); // record block num
    when(tx.getString(blk, 16)).thenReturn("test1"); // dataval
    when(tx.getInt(blk, 30)).thenReturn(102); // record slot id
    when(tx.getInt(blk, slotsize + 12)).thenReturn(20); // leaf block num
    when(tx.getString(blk, slotsize + 16)).thenReturn("test1"); // dataval
    when(tx.getInt(blk, slotsize + 30)).thenReturn(205); // record slot id

    // overflow block
    when(tx.getInt(overBlk, 0)).thenReturn(-1); // no overflow block
    when(tx.getInt(overBlk, 4)).thenReturn(2); // record num
    when(tx.getInt(overBlk, 12)).thenReturn(30); // record block num
    when(tx.getString(overBlk, 16)).thenReturn("test1"); // dataval
    when(tx.getInt(overBlk, 30)).thenReturn(112); // record slot id
    when(tx.getString(overBlk, slotsize + 16)).thenReturn("test2"); // dataval

    BTreeLeaf bLeaf = new BTreeLeaf(tx, blk, layout, new Constant("test1"));
    assertEquals(new RID(10, 102), bLeaf.getDataRid());
    assertTrue(bLeaf.next());
    assertEquals(new RID(20, 205), bLeaf.getDataRid());
    assertTrue(bLeaf.next());
    assertEquals(new RID(30, 112), bLeaf.getDataRid()); // from overflow blk
    assertFalse(bLeaf.next());
  }
}
