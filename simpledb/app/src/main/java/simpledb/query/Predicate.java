package simpledb.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import simpledb.plan.Plan;

/*
 * Predicate is a Boolean combination of terms.
 */
public class Predicate {
  private List<Term> terms = new ArrayList<>();

  /*
   * Create an empty predicate, corresponding to "true".
   */
  public Predicate() {
  }

  /*
   * Create a predicate containing a single term
   */
  public Predicate(Term t) {
    terms.add(t);
  }

  public void conjoinWith(Predicate pred) {
    terms.addAll(pred.terms);
  }

  public boolean isEmpty() {
    return terms.isEmpty();
  }

  /*
   * Return true if all the terms are satisfied
   * with the specified scan.
   */
  public boolean isSatisfied(Scan s) {
    for (Term t : terms)
      if (!t.isSatisfied(s))
        return false;
    return true;
  }

  public String toString() {
    Iterator<Term> iter = terms.iterator();
    if (!iter.hasNext())
      return "";
    String result = iter.next().toString();
    while (iter.hasNext())
      result += " and " + iter.next().toString();
    return result;
  }

  /*
   * The product of all the term's reduction factors
   */
  public int reductionFactor(Plan p) {
    int factor = 1;
    for (Term t : terms)
      factor *= t.reductionFactor(p);
    return factor;
  }

  /*
   * Determine if there is a term of the form "F=c"
   * where F is the specified field and c is some constant.
   * If true, return the constant, otherwise return null.
   */
  public Object equatesWithConstant(String fldname) {
    for (Term t : terms) {
      Constant c = t.equatesWithConstant(fldname);
      if (c != null)
        return c;
    }
    return null;
  }

  /*
   * Determine if there is a term of the form "F1=F2"
   * where F1 is the specified field and F2 is another.
   * If true, return the F2 field name, otherwise return null.
   */
  public String equatesWithField(String fldname) {
    for (Term t : terms) {
      String s = t.equatesWithField(fldname);
      if (s != null)
        return s;
    }
    return null;
  }
}
