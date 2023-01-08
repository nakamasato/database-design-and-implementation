package simpledb.materialize;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import simpledb.query.Constant;

public class CountFnTest {

  @Test
  public void testCountFn() {
    AggregationFn fn = new CountFn("fld");
    assertEquals("countoffld", fn.fieldName());
    fn.processFirst(null);
    assertEquals(new Constant(1), fn.value());
    fn.processNext(null);
    assertEquals(new Constant(2), fn.value());
  }
}
