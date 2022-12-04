package simpledb.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;

public class ParserTest {
  @Test
  public void testParseField() {
    String s = "a";
    Parser p = new Parser(s);
    String field = p.field();
    assertEquals("a", field);
  }

  @Test
  public void testParseConstantInt() {
    String s = "10";
    Parser p = new Parser(s);
    Constant cons = p.constant();
    assertEquals(10, cons.asInt());
  }

  @Test
  public void testParseConstantString() {
    String s = "'test'";
    Parser p = new Parser(s);
    Constant cons = p.constant();
    assertEquals("test", cons.asString());
  }

  @Test
  public void testParseExpressionField() {
    String s = "a";
    Parser p = new Parser(s);
    Expression exp = p.expression();
    assertTrue(exp.isFieldName()); // 'a' is a field
    assertEquals("a", exp.asFieldName()); // 'a' as field name
  }

  @Test
  public void testParseExpressionConstantString() {
    String s = "'test'";
    Parser p = new Parser(s);
    Expression exp = p.expression();
    assertFalse(exp.isFieldName());
    assertEquals(new Constant("test"), exp.asConstant());
  }

  @Test
  public void testParseExpressionConstantInt() {
    String s = "10";
    Parser p = new Parser(s);
    Expression exp = p.expression();
    assertFalse(exp.isFieldName());
    assertEquals(new Constant(10), exp.asConstant());
  }

  @Test
  public void testParseTerm() {
    String s = "a = 10";
    Parser p = new Parser(s);
    Term term = p.term();
    assertEquals("a=10", term.toString());
  }

  @Test
  public void testParsePredicate() {
    String s = "a = 10 AND b = 'test'";
    Parser p = new Parser(s);
    Predicate pred = p.predicate();
    assertEquals("a=10 and b=test", pred.toString());
  }
}
