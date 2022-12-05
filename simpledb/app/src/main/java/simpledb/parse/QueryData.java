package simpledb.parse;

import java.util.Collection;
import java.util.List;

import simpledb.query.Predicate;

/*
 * Data for the SQL select statement:
 * select <fields> from <tables> where <pred>
 */
public class QueryData {
  private List<String> fields;
  private Collection<String> tables;
  private Predicate pred;

  public QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
    this.fields = fields;
    this.tables = tables;
    this.pred = pred;
  }

  public List<String> fields() {
    return fields;
  }

  public Collection<String> tables() {
    return tables;
  }

  public Predicate predicate() {
    return pred;
  }

  public String toString() {
    String result = "select ";
    // fields
    result += String.join(", ", fields());
    result += " from ";
    // tables
    result += String.join(", ", tables());
    // where clause
    if (!pred.isEmpty())
      result += " where " + pred.toString();
    return result;
  }
}
