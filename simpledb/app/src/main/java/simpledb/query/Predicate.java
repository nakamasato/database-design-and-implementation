package simpledb.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
    return terms.size() == 0;
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

}
