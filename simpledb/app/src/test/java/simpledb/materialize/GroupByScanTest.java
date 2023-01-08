package simpledb.materialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.query.Constant;
import simpledb.query.Scan;

@ExtendWith(MockitoExtension.class)
public class GroupByScanTest {
  @Mock
  private Scan scan;

  @Mock
  private AggregationFn aggfn;

  @Test
  public void testGroupByScan() {
    when(scan.next()).thenReturn(true, true, true, true, false); // 4 records
    when(scan.getVal("gf")).thenReturn(new Constant(1),new Constant(1) , new Constant(2), new Constant(2));
    when(aggfn.fieldName()).thenReturn("countoffld");
    when(aggfn.value()).thenReturn(new Constant(10), new Constant(20));

    GroupByScan gbs = new GroupByScan(scan, Arrays.asList("gf"), Arrays.asList(aggfn));

    assertTrue(gbs.next()); // gf=1
    assertEquals(1, gbs.getInt("gf"));
    assertEquals(new Constant(10), gbs.getVal("countoffld"));
    assertTrue(gbs.next()); // gf=2
    assertEquals(2, gbs.getInt("gf"));
    assertEquals(new Constant(20), gbs.getVal("countoffld"));
    assertFalse(gbs.next());
  }
}
