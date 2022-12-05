package simpledb.parse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;

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
   */
  public Object updateCmd() {
    if (lex.matchKeyword("insert"))
      return insert();
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
}
