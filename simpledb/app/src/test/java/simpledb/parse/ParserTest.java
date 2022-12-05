package simpledb.parse;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

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

  @Test
  public void testParseQuery() {
    String s = "select a, b from tbl1, tbl2 where a = 10 and b = 'test'";
    Parser p = new Parser(s);
    QueryData qd = p.query();
    assertEquals(Arrays.asList("a", "b"), qd.fields());
    assertEquals(Arrays.asList("tbl1", "tbl2"), qd.tables());
    assertFalse(qd.predicate().isEmpty());
    assertEquals("select a, b from tbl1, tbl2 where a=10 and b=test", qd.toString());
  }

  @Test
  public void testParseInsert() {
    String s = "insert into tbl(a, b) values (10, 'test')";
    Parser p = new Parser(s);
    InsertData insertData = (InsertData) p.updateCmd();
    assertEquals(Arrays.asList("a", "b"), insertData.fields());
    assertEquals("tbl", insertData.tableName());
    assertEquals(new Constant(10), insertData.vals().get(0));
    assertEquals(new Constant("test"), insertData.vals().get(1));
  }

  @Test
  public void testParseDelete() {
    String s = "delete from tbl where a = 10 and b = 'test'";
    Parser p = new Parser(s);
    DeleteData deleteData = (DeleteData) p.updateCmd();
    assertEquals("tbl", deleteData.tableName());
    assertEquals("a=10 and b=test", deleteData.pred().toString());
  }

  @Test
  public void testParseModify() {
    String s = "update tbl set a = 10 where b = 'test'";
    Parser p = new Parser(s);
    ModifyData modifyData = (ModifyData) p.updateCmd();
    assertEquals("tbl", modifyData.tableName());
    assertEquals("a", modifyData.targetField());
    assertEquals(new Constant(10), modifyData.newValue().asConstant());
    assertEquals("b=test", modifyData.pred().toString());
  }

  @Test
  public void testParseCreateTable() {
    String s = "create table tbl (a int, b varchar(20))";
    Parser p = new Parser(s);
    CreateTableData createTableData = (CreateTableData) p.updateCmd();
    assertEquals("tbl", createTableData.tableName());
    assertEquals(INTEGER, createTableData.newSchema().type("a"));
    assertEquals(0, createTableData.newSchema().length("a"));
    assertEquals(VARCHAR, createTableData.newSchema().type("b"));
    assertEquals(20, createTableData.newSchema().length("b"));
  }

  @Test
  public void testParseCreateView() {
    String s = "create view view_name as select a, b from tbl where a = 10 and b = 'test'";
    Parser p = new Parser(s);
    CreateViewData createViewData = (CreateViewData) p.updateCmd();
    assertEquals("view_name", createViewData.viewName());
    assertEquals("select a, b from tbl where a=10 and b=test", createViewData.viewDef());
  }

  @Test
  public void testParseCreateIndex() {
    String s = "create index test_idx on tbl(a)";
    Parser p = new Parser(s);
    CreateIndexData createIndexData = (CreateIndexData) p.updateCmd();
    assertEquals("test_idx", createIndexData.indexName());
    assertEquals("tbl", createIndexData.tableName());
    assertEquals("a", createIndexData.fieldName());
  }
}
