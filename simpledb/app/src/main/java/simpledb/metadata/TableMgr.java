package simpledb.metadata;

import java.util.HashMap;
import java.util.Map;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

/*
 * The table manager.
 * There are methods to create a table, save the metadata
 * in the catalog, and obtain the metadata of a
 * previously-created table.
 */
public class TableMgr {
  // The max characters a tablename or fieldname can have.
  public static final int MAX_NAME = 16;

  // Table name and field name for table catalog
  public static final String TBL_CAT_TABLE = "tblcat";
  public static final String TBL_CAT_FIELD_TABLE_NAME = "tblname";
  public static final String TBL_CAT_FIELD_SLOTSIZE = "slotsize";

  // Table name and field name for field catalog
  public static final String FLD_CAT_TABLE = "fldcat";
  public static final String FLD_CAT_FIELD_TABLE_NAME = "tblname";
  public static final String FLD_CAT_FIELD_FEILD_NAME = "fldname";
  public static final String FLD_CAT_FIELD_LENGTH = "length";
  public static final String FLD_CAT_FIELD_TYPE = "type";
  public static final String FLD_CAT_FIELD_OFFSET = "offset";

  // Layout for table catalog
  private Layout tcatLayout;
  // Layout for field catalog
  private Layout fcatLayout;

  /*
   * Constructor of TableMgr
   * If the database is new, create catalog tables
   * for table `tblcat` and field `fldcat`.
   */
  public TableMgr(boolean isNew, Transaction tx) {
    Schema tcatSchema = new Schema();
    tcatSchema.addStringField(TBL_CAT_FIELD_TABLE_NAME, MAX_NAME);
    tcatSchema.addIntField(TBL_CAT_FIELD_SLOTSIZE);
    tcatLayout = new Layout(tcatSchema);

    Schema fcatSchema = new Schema();
    fcatSchema.addStringField(FLD_CAT_FIELD_TABLE_NAME, MAX_NAME);
    fcatSchema.addStringField(FLD_CAT_FIELD_FEILD_NAME, MAX_NAME);
    fcatSchema.addIntField(FLD_CAT_FIELD_TYPE);
    fcatSchema.addIntField(FLD_CAT_FIELD_LENGTH);
    fcatSchema.addIntField(FLD_CAT_FIELD_OFFSET);
    fcatLayout = new Layout(fcatSchema);

    if (isNew) {
      createTable(TBL_CAT_TABLE, tcatSchema, tx);
      createTable(FLD_CAT_TABLE, fcatSchema, tx);
    }
  }

  /*
   * Create a table.
   * Insert a catalog record to `tblcat` and `fldcat` tables.
   */
  public void createTable(String tblname, Schema sch, Transaction tx) {
    System.out.println("[TableMgr] createTable table: " + tblname);
    Layout layout = new Layout(sch);

    // insert one record into tblcat
    TableScan tcat = new TableScan(tx, TBL_CAT_TABLE, tcatLayout);
    tcat.insert();
    tcat.setString(TBL_CAT_FIELD_TABLE_NAME, tblname);
    tcat.setInt(TBL_CAT_FIELD_SLOTSIZE, layout.slotSize());
    tcat.close();

    // insert a record into fldcat for each field
    TableScan fcat = new TableScan(tx, FLD_CAT_TABLE, fcatLayout);
    for (String fldname : sch.fields()) {
      fcat.insert();
      fcat.setString(FLD_CAT_FIELD_TABLE_NAME, tblname);
      fcat.setString(FLD_CAT_FIELD_FEILD_NAME, fldname);
      fcat.setInt(FLD_CAT_FIELD_TYPE, sch.type(fldname));
      fcat.setInt(FLD_CAT_FIELD_LENGTH, sch.length(fldname));
      fcat.setInt(FLD_CAT_FIELD_OFFSET, layout.offset(fldname));
    }
    System.out.println("[TableMgr] createTable completed table: " + tblname);
  }

  public Layout getLayout(String tblname, Transaction tx) {
    int size = -1;
    TableScan tcat = new TableScan(tx, TBL_CAT_TABLE, tcatLayout);
    while (tcat.next())
      if (tcat.getString(TBL_CAT_FIELD_TABLE_NAME).equals(tblname)) {
        size = tcat.getInt(TBL_CAT_FIELD_SLOTSIZE);
        break;
      }
    tcat.close();

    Schema sch = new Schema();
    Map<String, Integer> offsets = new HashMap<>();
    TableScan fcat = new TableScan(tx, FLD_CAT_TABLE, fcatLayout);
    while (fcat.next())
      if (fcat.getString(FLD_CAT_FIELD_TABLE_NAME).equals(tblname)) {
        String fldname = fcat.getString(FLD_CAT_FIELD_FEILD_NAME);
        int fldtype = fcat.getInt(FLD_CAT_FIELD_TYPE);
        int fldlen = fcat.getInt(FLD_CAT_FIELD_LENGTH);
        int offset = fcat.getInt(FLD_CAT_FIELD_OFFSET);
        offsets.put(fldname, offset);
        sch.addField(fldname, fldtype, fldlen);
      }
    fcat.close();
    return new Layout(sch, offsets, size);
  }
}
