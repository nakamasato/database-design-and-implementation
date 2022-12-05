package simpledb.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;

public class QueryDataTest {
  @Test
  void testToStringWithFieldsAndTables() {
    Predicate pred = new Predicate();
    QueryData qd = new QueryData(Arrays.asList("f1", "f2", "f3"), Arrays.asList("tbl1", "tbl2"), pred);
    String expectedString = "select f1, f2, f3 from tbl1, tbl2";
    assertEquals(expectedString, qd.toString());
  }

  @Test
  void testToStringWithPredicate() {
    Term t = new Term(new Expression("f1"), new Expression("f2"));
    Predicate pred = new Predicate(t);
    QueryData qd = new QueryData(Arrays.asList("f1", "f2", "f3"), Arrays.asList("tbl1", "tbl2"), pred);
    String expectedString = "select f1, f2, f3 from tbl1, tbl2 where f1=f2";
    assertEquals(expectedString, qd.toString());
  }
}
