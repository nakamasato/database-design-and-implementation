package simpledb.query;

import simpledb.record.Schema;

/*
 * A term is a comparison between two expressions.
 * Now only supports equality
 */
public class Term {
  private Expression lhs;
  private Expression rhs;

  public Term(Expression lhs, Expression rhs) {
    this.lhs = lhs;
    this.rhs = rhs;
  }

  /*
   * Return true if the evaluation of the expressions
   * are equal.
   * This function is used to determined the result of Predicate.
   */
  public boolean isSatisfied(Scan s) {
    Constant lhsval = lhs.evaluate(s);
    Constant rhsval = rhs.evaluate(s);
    return rhsval.equals(lhsval);
  }

  /*
   * Return true if both of the term's expressions
   * apply to the speicified schema.
   */
  public boolean appliesTo(Schema sch) {
    return lhs.appliesTo(sch) && rhs.appliesTo(sch);
  }

  public String toString() {
    return lhs.toString() + "=" + rhs.toString();
  }
}
