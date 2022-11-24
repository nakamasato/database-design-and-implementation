package simpledb.record;

import static java.sql.Types.INTEGER;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.Page;

/*
 * Description of the structure of a record.
 * It contains the name, type, length and offset of
 * each field of the table.
 */
public class Layout {
  private Schema schema;
  /*
   * Offset of each field
   */
  private Map<String, Integer> offsets;
  private int slotsize;

  public Layout(Schema schema) {
    this.schema = schema;
    offsets = new HashMap<>();
    int pos = Integer.BYTES; // leave space for the empty/inuse flag
    for (String fldname : schema.fields()) {
      offsets.put(fldname, pos);
      pos += lengthInBytes(fldname);
    }
    slotsize = pos;
  }

  public Layout(Schema schema, Map<String, Integer> offsets, int slotsize) {
    this.schema = schema;
    this.offsets = offsets;
    this.slotsize = slotsize;
  }

  public Schema schema() {
    return schema;
  }

  public int offset(String fldname) {
    return offsets.get(fldname);
  }

  public int slotSize() {
    return slotsize;
  }

  private int lengthInBytes(String fldname) {
    int fldtype = schema.type(fldname);
    if (fldtype == INTEGER)
      return Integer.BYTES;
    else
      return Page.maxLength(schema.length(fldname));
  }
}
