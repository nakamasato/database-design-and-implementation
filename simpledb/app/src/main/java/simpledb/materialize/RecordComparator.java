package simpledb.materialize;

import java.util.Comparator;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;

/*
 * A comparator for Scans with the specified fields
 */
public class RecordComparator implements Comparator<Scan> {
  private List<String> fields;

  public RecordComparator(List<String> fields) {
    this.fields = fields;
  }

  /*
   * Compare the current records of the two specified scans
   */
  @Override
  public int compare(Scan s1, Scan s2) {
    for (String fldname : fields) {
      Constant val1 = s1.getVal(fldname);
      Constant val2 = s2.getVal(fldname);
      int result = val1.compareTo(val2);
      if (result != 0)
        return result;
    }
    return 0;
  }
}
