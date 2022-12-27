package simpledb.record;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * The record schema of a table.
 * A schema contains the name, type, and the length of varchar field
 */
public class Schema {
  private List<String> fields = new ArrayList<>();
  private Map<String, FieldInfo> info = new HashMap<>();

  public void addField(String fldname, int type, int length) {
    fields.add(fldname);
    info.put(fldname, new FieldInfo(type, length));
  }

  public void addIntField(String fldname) {
    addField(fldname, INTEGER, 0);
  }

  public void addStringField(String fldname, int length) {
    addField(fldname, VARCHAR, length);
  }

  /*
   * add existing schema's field to this shema
   */
  public void add(String fldname, Schema schema) {
    int type = schema.type(fldname);
    int length = schema.length(fldname);
    addField(fldname, type, length);
  }

  public void addAll(Schema schema) {
    for (String fldname : schema.fields())
      add(fldname, schema);
  }

  public List<String> fields() {
    return fields;
  }

  public boolean hasField(String fldname) {
    return fields.contains(fldname);
  }

  public int type(String fldname) {
    return info.get(fldname).type;
  }

  public int length(String fldname) {
    return info.get(fldname).length;
  }

  class FieldInfo {
    int type;
    int length;

    public FieldInfo(int type, int length) {
      this.type = type;
      this.length = length;
    }
  }
}
