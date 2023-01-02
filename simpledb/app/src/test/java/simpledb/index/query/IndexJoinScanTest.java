package simpledb.index.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.index.Index;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.record.TableScan;

@ExtendWith(MockitoExtension.class)
public class IndexJoinScanTest {
  @Mock
  private TableScan lhs;
  @Mock
  private TableScan rhs;
  @Mock
  private Index idx;

  /*
   * T1: RHS
   * fld1 |
   * ---|
   * 1 |
   * 2 |
   * T2: LHS
   * fld2|
   * test1|
   * test2|
   * join(T1, T2)
   * fld1|fld2
   * 1 | test1
   * 2 | test1
   * 1 | test2
   * 2 | test2
   */
  @Test
  public void testIndexJoinScan() {
    RID rid1 = new RID(0, 0);
    RID rid2 = new RID(0, 1);
    when(idx.next()).thenReturn(true, true, true, true, false); // two records twice for index
    when(idx.getDataRid()).thenReturn(rid1, rid2);
    when(rhs.hasField("T1_fld1")).thenReturn(true);
    when(rhs.getInt("T1_fld1")).thenReturn(1, 2, 1, 2);
    when(lhs.next()).thenReturn(true, true, false); // two record
    when(lhs.getString("T2_fld2")).thenReturn("test1", "test1", "test2", "test2");
    String joinfield = "joinfield";
    Scan s = new IndexJoinScan(lhs, idx, joinfield, rhs);
    assertTrue(s.next()); // first lhs record x first index record
    assertEquals(1, s.getInt("T1_fld1"));
    assertEquals("test1", s.getString("T2_fld2"));
    assertTrue(s.next()); // first lhs record x second index record
    assertEquals(2, s.getInt("T1_fld1"));
    assertEquals("test1", s.getString("T2_fld2"));
    assertTrue(s.next()); // second lhs record x first index record
    assertEquals(1, s.getInt("T1_fld1"));
    assertEquals("test2", s.getString("T2_fld2"));
    assertTrue(s.next()); // second lhs record x second index record
    assertEquals(2, s.getInt("T1_fld1"));
    assertEquals("test2", s.getString("T2_fld2"));
    assertFalse(s.next());
  }
}
