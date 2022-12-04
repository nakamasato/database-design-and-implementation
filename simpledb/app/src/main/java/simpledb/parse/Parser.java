package simpledb.parse;

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
}
