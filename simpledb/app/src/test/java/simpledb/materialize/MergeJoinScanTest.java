package simpledb.materialize;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.query.Constant;
import simpledb.query.Scan;

@ExtendWith(MockitoExtension.class)
public class MergeJoinScanTest {
  @Mock
  private Scan s1;

  @Mock
  private SortScan s2;

  @Test
  public void testMergeJoinScan() {
    when(s1.getVal("joinfield")).thenReturn(new Constant(1));
    when(s1.next()).thenReturn(true, false);
    when(s2.getVal("joinfield")).thenReturn(new Constant(1), new Constant(1));
    when(s2.next()).thenReturn(true, true, false);
    MergeJoinScan scan = new MergeJoinScan(s1, s2, "joinfield", "joinfield");
    scan.beforeFirst();
    assertTrue(scan.next()); // extract first record from s1 and s2 -> joinfield=1
    assertTrue(scan.next()); // s2.next -> joinfield=1
    assertFalse(scan.next()); // s1.next() is false, s2.next() is false.
  }
}
