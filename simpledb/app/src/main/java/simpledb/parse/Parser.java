package simpledb.parse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;

public class Parser {
  private Lexer lex;

  public Parser(String s) {
    lex = new Lexer(s);
  }

  // Methods for parsing predicates, terms, expressions, constants, and fields

  /*
   * move to next token and return the id
   */
  public String field() {
    return lex.eatId();
  }

  /*
   * Return new constant with string if match a string constant.
   * Otherwise return new constant with an integer.
   */
  public Constant constant() {
    if (lex.matchStringConstant())
      return new Constant(lex.eatStringConstant());
    else
      return new Constant(lex.eatIntConstant());
  }

  /*
   * Return new expression based on matchedId
   * use fields If matches id, otherwise constant
   */
  public Expression expression() {
    if (lex.matchId())
      return new Expression(field());
    else
      return new Expression(constant());
  }

  /*
   * term only supports equality comparison
   */
  public Term term() {
    Expression lhs = expression();
    lex.eatDelim('=');
    Expression rhs = expression();
    return new Term(lhs, rhs);
  }

  public Predicate predicate() {
    Predicate pred = new Predicate(term());
    if (lex.matchKeyword("and")) {
      lex.eatKeyword("and");
      pred.conjoinWith(predicate());
    }
    return pred;
  }

  // Methods for parsing queries

  public QueryData query() {
    lex.eatKeyword("select");
    List<String> fields = selectList();
    lex.eatKeyword("from");
    Collection<String> tables = tableList();
    Predicate pred = new Predicate();
    if (lex.matchKeyword("where")) {
      lex.eatKeyword("where");
      pred = predicate();
    }
    return new QueryData(fields, tables, pred);
  }

  private List<String> selectList() {
    List<String> L = new ArrayList<>();
    L.add(field());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(selectList());
    }
    return L;
  }

  private Collection<String> tableList() {
    Collection<String> L = new ArrayList<>();
    L.add(lex.eatId());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(tableList());
    }
    return L;
  }

  // Methods for parsing the various update commands

  /*
   * Call corresponding private method based on the matched keyword:
   * 1. insert: insert()
   * 2. delete: delete()
   * 3. update: modify()
   * 4. create: create()
   * 5. else: BadSyntaxExceptiion()
   */
  public Object updateCmd() {
    if (lex.matchKeyword("insert"))
      return insert();
    if (lex.matchKeyword("delete"))
      return delete();
    if (lex.matchKeyword("update"))
      return modify();
    if (lex.matchKeyword("create"))
      return create();
    else
      throw new BadSyntaxException();
  }

  /*
   * Parse insert SQL and return InsertData object
   */
  private InsertData insert() {
    lex.eatKeyword("insert");
    lex.eatKeyword("into");
    String tblname = lex.eatId();
    lex.eatDelim('(');
    List<String> flds = fieldList();
    lex.eatDelim(')');
    lex.eatKeyword("values");
    lex.eatDelim('(');
    List<Constant> vals = constList();
    lex.eatDelim(')');
    return new InsertData(tblname, flds, vals);
  }

  private List<String> fieldList() {
    List<String> L = new ArrayList<>();
    L.add(field());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(fieldList());
    }
    return L;
  }

  private List<Constant> constList() {
    List<Constant> L = new ArrayList<>();
    L.add(constant());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(constList());
    }
    return L;
  }

  /*
   * Parse delete SQL and return DeleteData object
   */
  public DeleteData delete() {
    lex.eatKeyword("delete");
    lex.eatKeyword("from");
    String tblname = lex.eatId();
    Predicate pred = new Predicate();
    if (lex.matchKeyword("where")) {
      lex.eatKeyword("where");
      pred = predicate();
    }
    return new DeleteData(tblname, pred);
  }

  /*
   * Parse update SQL and return ModifyData object
   */
  public ModifyData modify() {
    lex.eatKeyword("update");
    String tblname = lex.eatId();
    lex.eatKeyword("set");
    String fldname = field();
    lex.eatDelim('=');
    Expression newval = expression();
    Predicate pred = new Predicate();
    if (lex.matchKeyword("where")) {
      lex.eatKeyword("where");
      pred = predicate();
    }
    return new ModifyData(tblname, fldname, newval, pred);
  }

  public Object create() {
    lex.eatKeyword("create");
    if (lex.matchKeyword("table"))
      return createTable();
    else
      throw new BadSyntaxException();
  }

  /*
   * Parse create table SQL and return CreateTableData object
   * SQL: CREATE TABLE <tablename> (fld1 int, fld2 varchar(20))
   */
  public CreateTableData createTable() {
    lex.eatKeyword("table");
    String tblname = lex.eatId();
    lex.eatDelim('(');
    Schema sch = fieldDefs();
    lex.eatDelim(')');
    return new CreateTableData(tblname, sch);
  }

  private Schema fieldDefs() {
    Schema schema = fieldDef();
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      Schema schema2 = fieldDefs();
      schema.addAll(schema2);
    }
    return schema;
  }

  private Schema fieldDef() {
    String fldname = field();
    return fieldType(fldname);
  }

  /*
   * Extract field type (int or varchar) for the given field name
   * and add a field with the field type
   */
  private Schema fieldType(String fldname) {
    Schema schema = new Schema();
    if (lex.matchKeyword("int")) {
      lex.eatKeyword("int");
      schema.addIntField(fldname);
    } else if (lex.matchKeyword("varchar")) {
      lex.eatKeyword("varchar");
      lex.eatDelim('(');
      int strlen = lex.eatIntConstant();
      lex.eatDelim(')');
      schema.addStringField(fldname, strlen);
    } else {
      throw new BadSyntaxException();
    }
    return schema;
  }
}
