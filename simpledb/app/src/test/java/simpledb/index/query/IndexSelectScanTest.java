package simpledb.index.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.RID;
import simpledb.record.TableScan;

@ExtendWith(MockitoExtension.class)
public class IndexSelectScanTest {
  @Mock
  private TableScan ts;

  @Mock
  private Index idx;

  @Test
  public void testIndexSelectScan() {
    Constant val = new Constant("test");
    when(ts.getInt("fldint")).thenReturn(1);
    when(ts.getString("fldstr")).thenReturn("test");
    when(ts.getVal("fldval")).thenReturn(val);
    when(ts.hasField("fld")).thenReturn(true);
    when(idx.next()).thenReturn(true, true, false);
    when(idx.getDataRid()).thenReturn(new RID(1, 1), new RID(1, 2));

    IndexSelectScan idxSelectScan = new IndexSelectScan(ts, idx, val);

    verify(idx).beforeFirst(val);

    while (idxSelectScan.next())
      idxSelectScan.getInt("fldint");

    verify(idx, times(3)).next();
    verify(idx, times(2)).getDataRid();

    assertEquals(1, idxSelectScan.getInt("fldint"));
    assertEquals("test", idxSelectScan.getString("fldstr"));
    assertEquals(val, idxSelectScan.getVal("fldval"));
    assertEquals(true, idxSelectScan.hasField("fld"));

    idxSelectScan.close();
    verify(idx).close();
    verify(ts).close();
  }
}
