package simpledb.materialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.query.Constant;
import simpledb.query.Scan;

@ExtendWith(MockitoExtension.class)
public class RecordComparatorTest {
  @Mock
  private Scan s1;

  @Mock
  private Scan s2;

  @Test
  public void testCompareForEqualCase() {
    when(s1.getVal("fld1")).thenReturn(new Constant(1));
    when(s2.getVal("fld1")).thenReturn(new Constant(1));
    RecordComparator comp = new RecordComparator(Arrays.asList("fld1"));
    assertEquals(0, comp.compare(s1, s2));
  }

  @Test
  public void testCompareForLargerCase() {
    when(s1.getVal("fld1")).thenReturn(new Constant(10));
    when(s2.getVal("fld1")).thenReturn(new Constant(1));
    RecordComparator comp = new RecordComparator(Arrays.asList("fld1"));
    assertEquals(1, comp.compare(s1, s2));
  }

  @Test
  public void testCompareForSmallerCase() {
    when(s1.getVal("fld1")).thenReturn(new Constant(0));
    when(s2.getVal("fld1")).thenReturn(new Constant(1));
    RecordComparator comp = new RecordComparator(Arrays.asList("fld1"));
    assertEquals(-1, comp.compare(s1, s2));
  }
}
