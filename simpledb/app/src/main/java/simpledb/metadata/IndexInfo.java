package simpledb.metadata;

import static java.sql.Types.INTEGER;

import simpledb.index.Index;
import simpledb.index.btree.BTreeIndex;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class IndexInfo {
  private String idxname;
  private String fldname;
  private Transaction tx;
  private Schema tblSchema;
  private Layout idxLayout;
  private StatInfo si;

  public IndexInfo(String idxname, String fldname, Schema tblSchema, Transaction tx, StatInfo si) {
    this.idxname = idxname;
    this.fldname = fldname;
    this.tx = tx;
    this.tblSchema = tblSchema;
    this.idxLayout = createIdxLayout();
    this.si = si;
  }

  public Index open() {
    return new BTreeIndex(tx, idxname, idxLayout);
  }

  public int blocksAccessed() {
    int rpb = tx.blockSize() / idxLayout.slotSize();
    int numBlocks = si.recordsOutput() / rpb;
    return BTreeIndex.searchCost(numBlocks, rpb);
  }

  /*
   * Return the estimated number of records having a search key.
   * The number of distinct values of the indexed fields.
   * This estimate will be very poor if not evenly distributed.
   */
  public int recordsOutput() {
    return si.recordsOutput() / si.distinctValues(fldname);
  }

  /*
   * Return the distinct values for a specified field or 1 for the indexed field
   */
  public int distinctValues(String fname) {
    return fldname.equals(fname) ? 1 : si.distinctValues(fldname);
  }

  /*
   * Return the layout of the index records.
   * The schema consists of
   * 1. RID (the block number and record id)
   * 2. dataval: the type is determined based on the fldname
   */
  private Layout createIdxLayout() {
    Schema sch = new Schema();
    sch.addIntField("block");
    sch.addIntField("id");
    if (tblSchema.type(fldname) == INTEGER)
      sch.addIntField("dataval");
    else {
      int fldlen = tblSchema.length(fldname);
      sch.addStringField("dataval", fldlen);
    }
    return new Layout(sch);
  }
}
