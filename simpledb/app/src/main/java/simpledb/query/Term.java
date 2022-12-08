package simpledb.query;

import simpledb.plan.Plan;
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

  /*
   * Calculate the extent to which selecting on the term reduces the number of
   * records output by a query.
   * If reduction factor is 2, the term cuts the size in half.
   */
  public int reductionFactor(Plan p) {
    String lhsName;
    String rhsName;
    // max of 1/(distinct values of the field)
    if (lhs.isFieldName() && rhs.isFieldName()) {
      lhsName = lhs.asFieldName();
      rhsName = rhs.asFieldName();
      return Math.max(p.distinctValues(lhsName), p.distinctValues(rhsName));
    }
    if (lhs.isFieldName()) {
      lhsName = lhs.asFieldName();
      return p.distinctValues(lhsName); // 1/(distinct values of the field)
    }
    if (rhs.isFieldName()) {
      rhsName = rhs.asFieldName();
      return p.distinctValues(rhsName); // 1/(distinct values of the field)
    }
    if (lhs.asConstant().equals(rhs.asConstant())) // no change
      return 1;
    else
      return Integer.MAX_VALUE; // not match -> infinite reduction
  }

  /*
   * If the term is in the form of F=c, return c
   * otherwise, return null.
   */
  public Constant equatesWithConstant(String fldname) {
    if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && !rhs.isFieldName())
      return rhs.asConstant();
    else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && !lhs.isFieldName())
      return lhs.asConstant();
    else
      return null;
  }

  /*
   * If the term is in the form of F1=F2, return the field name
   * otherwise, return null
   */
  public String equatesWithField(String fldname) {
    if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && rhs.isFieldName())
      return rhs.asFieldName();
    else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && lhs.isFieldName())
      return lhs.asFieldName();
    else
      return null;
  }
}
