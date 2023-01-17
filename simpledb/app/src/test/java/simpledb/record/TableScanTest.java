package simpledb.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.file.BlockId;
import simpledb.query.Scan;
import simpledb.tx.Transaction;

@ExtendWith(MockitoExtension.class)
public class TableScanTest {
  @Mock
  private Transaction tx;

  /*
   * pin/unpin every time the scan reads value
   * you can compare with ChunkScanTest
   */
  @Test
  public void testTableScan() {
    String tblname = "test_tbl";
    String filename = tblname + ".tbl";
    Schema sch = new Schema();
    sch.addIntField("intfld");
    Layout layout = new Layout(sch);
    when(tx.blockSize()).thenReturn(16);
    when(tx.size(filename)).thenReturn(2);
    when(tx.getInt(new BlockId(filename, 0), 0)).thenReturn(1); // flag
    when(tx.getInt(new BlockId(filename, 0), 4)).thenReturn(1); // intfld
    when(tx.getInt(new BlockId(filename, 0), 0 + layout.slotSize())).thenReturn(1); // flag
    when(tx.getInt(new BlockId(filename, 0), 4 + layout.slotSize())).thenReturn(2); // intfld
    when(tx.getInt(new BlockId(filename, 1), 0)).thenReturn(1); // flag
    when(tx.getInt(new BlockId(filename, 1), 4)).thenReturn(3); // intfld
    when(tx.getInt(new BlockId(filename, 1), 0 + layout.slotSize())).thenReturn(0); // flag

    Scan scan = new TableScan(tx, tblname, layout);
    verify(tx, times(1)).pin(new BlockId(filename, 0));
    scan.beforeFirst();

    // read first time
    assertTrue(scan.next());
    assertEquals(1, scan.getInt("intfld"));
    assertTrue(scan.next());
    assertEquals(2, scan.getInt("intfld"));
    assertTrue(scan.next());
    assertEquals(3, scan.getInt("intfld"));
    assertFalse(scan.next());
    verify(tx, times(3)).pin(any(BlockId.class));
    verify(tx, times(2)).unpin(any(BlockId.class));

    // read second time
    scan.beforeFirst();
    assertTrue(scan.next());
    assertEquals(1, scan.getInt("intfld"));
    assertTrue(scan.next());
    assertEquals(2, scan.getInt("intfld"));
    assertTrue(scan.next());
    assertEquals(3, scan.getInt("intfld"));
    assertFalse(scan.next());
    verify(tx, times(5)).pin(any(BlockId.class));
    verify(tx, times(4)).unpin(any(BlockId.class));

    scan.close();
    verify(tx, times(5)).unpin(any(BlockId.class));
  }
}
