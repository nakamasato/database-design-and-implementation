package simpledb.multibuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.file.BlockId;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

@ExtendWith(MockitoExtension.class)
public class ChunkScanTest {
  // tx, filenam, layout, startblk, endblk
  // tx.pin unpin only once

  @Mock
  private Transaction tx;

  @Test
  public void testChunkScanInit() {
    String filename = "filename";
    int startblk = 0;
    int endblk = 4;
    Schema sch = new Schema();
    sch.addIntField("intfld");
    Layout layout = new Layout(sch);
    new ChunkScan(tx, filename, layout, startblk, endblk);
    for (int i = startblk; i <= endblk; i++) {
      verify(tx).pin(new BlockId(filename, i)); // pin all blocks
      verify(tx, never()).unpin(new BlockId(filename, i)); // never unpin any blocks
    }
  }

  /*
   * pin/unpin only twice for three records in two blocks.
   * you can compare with TableScanTest
   */
  @Test
  public void testChunkScanGetInt() {
    String filename = "filename";
    int startblk = 0;
    int endblk = 1;
    Schema sch = new Schema();
    sch.addIntField("intfld");
    Layout layout = new Layout(sch);
    when(tx.blockSize()).thenReturn(16);
    when(tx.getInt(new BlockId(filename, startblk), 0)).thenReturn(1); // flag
    when(tx.getInt(new BlockId(filename, startblk), 4)).thenReturn(1); // intfld
    when(tx.getInt(new BlockId(filename, startblk), 0 + layout.slotSize())).thenReturn(1); // flag
    when(tx.getInt(new BlockId(filename, startblk), 4 + layout.slotSize())).thenReturn(2); // intfld
    when(tx.getInt(new BlockId(filename, startblk + 1), 0)).thenReturn(1); // flag
    when(tx.getInt(new BlockId(filename, startblk + 1), 4)).thenReturn(3); // intfld

    Scan scan = new ChunkScan(tx, filename, layout, startblk, endblk);
    // when initializing ChunkScan, it already pins the specified range of blocks
    verify(tx, times(2)).pin(any(BlockId.class));

    // read first time
    assertTrue(scan.next());
    assertEquals(1, scan.getInt("intfld"));
    assertTrue(scan.next());
    assertEquals(2, scan.getInt("intfld"));
    assertTrue(scan.next());
    assertEquals(3, scan.getInt("intfld"));
    assertFalse(scan.next());

    // read second time
    scan.beforeFirst();
    assertTrue(scan.next());
    assertEquals(1, scan.getInt("intfld"));
    assertTrue(scan.next());
    assertEquals(2, scan.getInt("intfld"));
    assertTrue(scan.next());
    assertEquals(3, scan.getInt("intfld"));
    assertFalse(scan.next());

    // no additional pin when getting values
    verify(tx, times(2)).pin(any(BlockId.class));
    verify(tx, never()).unpin(any(BlockId.class));

    scan.close();
    verify(tx, times(2)).unpin(any(BlockId.class));
  }

  @Test
  public void testChunkScanClose() {
    String filename = "filename";
    int startblk = 0;
    int endblk = 4;
    Schema sch = new Schema();
    sch.addIntField("intfld");
    Layout layout = new Layout(sch);
    Scan scan = new ChunkScan(tx, filename, layout, startblk, endblk);
    verify(tx, never()).unpin(any(BlockId.class));
    scan.close();
    for (int i = startblk; i <= endblk; i++) {
      verify(tx).unpin(new BlockId(filename, i)); // unpin all blocks
    }
  }
}
