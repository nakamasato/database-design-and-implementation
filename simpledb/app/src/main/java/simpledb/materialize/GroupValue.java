package simpledb.materialize;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.query.Constant;
import simpledb.query.Scan;

/*
 * Object to hold the values of the grouping fields for
 * the current record of a scan.
 */
public class GroupValue {
  private Map<String, Constant> vals = new HashMap<>();

  public GroupValue(Scan s, List<String> fields) {
    vals = new HashMap<>();
    for (String fldname : fields)
      vals.put(fldname, s.getVal(fldname));
  }

  public Constant getVal(String fldname) {
    return vals.get(fldname);
  }

  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    GroupValue gv = (GroupValue) obj;
    for (Map.Entry<String, Constant> e : vals.entrySet()) {
      Constant v1 = e.getValue();
      Constant v2 = gv.getVal(e.getKey());
      if (!v1.equals(v2))
        return false;
    }
    return true;
  }

  /*
   * The hashcode of a GroupValue object is
   * the sum of the hashcodes of its field values.
   */
  public int hashCode() {
    int hashval = 0;
    for (Constant c : vals.values())
      hashval += c.hashCode();
    return hashval;
  }
}
