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
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

@ExtendWith(MockitoExtension.class)
public class BTPageTest {
  private static final String filename = "testidx";
  @Mock
  private Transaction tx;

  /*
   * test BTPage methods for BTreeDir with the following data
   * |block|dataval|
   * |10|"test1"|
   * |20|"test2"|
   */
  @Test
  public void testBTPageForBTreeDirMethods() {
    BlockId blk = new BlockId(filename, 0);
    Schema sch = new Schema();
    sch.addIntField("block"); // blk num
    sch.addStringField("dataval", 10); // index on String field
    Layout layout = new Layout(sch);
    int slotsize = layout.slotSize();
    when(tx.getInt(blk, 0)).thenReturn(-1); // flag
    when(tx.getInt(blk, 4)).thenReturn(2); // record num
    when(tx.getInt(blk, 12)).thenReturn(10); // leaf block num
    when(tx.getString(blk, 16)).thenReturn("test1"); // dataval = "test1"
    when(tx.getInt(blk, slotsize + 12)).thenReturn(20); // leaf block num
    when(tx.getString(blk, slotsize + 16)).thenReturn("test2"); // dataval = "tes2"

    BTPage page = new BTPage(tx, blk, layout);

    assertEquals(-1, page.getFlag());
    assertEquals(2, sch.fields().size());
    assertEquals(4 + 4 + 10 + 4, slotsize); // flag, int field, length + varchar(10)
    assertEquals(2, page.getNumRecs());
    assertEquals(-1, page.findSlotBefore(new Constant("test1")));
    assertEquals(0, page.findSlotBefore(new Constant("test2")));
    assertEquals(new Constant("test1"), page.getDataVal(0));
    assertEquals(new Constant("test2"), page.getDataVal(1));
    assertEquals(10, page.getChildNum(0));
    assertEquals(20, page.getChildNum(1));
  }

  /*
   * test BTPage methods for BTreeLeaf with the following data
   * |block|dataval|id|
   * |10|"test1"|102| <- record in (blk:10, slot:102) has dataval "test1"
   * |20|"test2"|205| <- record in (blk:20, slot:205) has dataval "test2"
   */
  @Test
  public void testBTPageForBTreeLeafMethods() {
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
    when(tx.getInt(blk, 30)).thenReturn(102); // record slot id
    when(tx.getInt(blk, slotsize + 12)).thenReturn(20); // leaf block num
    when(tx.getInt(blk, slotsize + 30)).thenReturn(205); // record slot id

    BTPage page = new BTPage(tx, blk, layout);

    assertEquals(-1, page.getFlag());
    assertEquals(3, sch.fields().size());
    assertEquals(4 + 4 + 10 + 4 + 4, slotsize); // flag, int field, length + varchar(10), int
    assertEquals(2, page.getNumRecs());
    assertEquals(new RID(10, 102), page.getDataRid(0));
    assertEquals(new RID(20, 205), page.getDataRid(1));
  }
}
