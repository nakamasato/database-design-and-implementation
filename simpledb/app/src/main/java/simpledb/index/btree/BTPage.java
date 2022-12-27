package simpledb.index.btree;

import static java.sql.Types.INTEGER;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/*
 * BTPage provides the common logic for BTreeLeaf and BTreeDir:
 * 1. store records in sorted order
 * 2. split a block when it's full
 */
public class BTPage {
  private static String FLD_NAME_DATAVAL = "dataval";
  private static String FLD_NAME_BLOCK = "block";
  private static String FLD_NAME_ID = "id";
  private Transaction tx;
  private BlockId currentblk;
  private Layout layout;

  public BTPage(Transaction tx, BlockId currentblk, Layout layout) {
    this.tx = tx;
    this.currentblk = currentblk;
    this.layout = layout;
    tx.pin(currentblk);
  }

  /*
   * Return the slot position where the first record having the specified
   * search key should be.
   */
  public int findSlotBefore(Constant searchkey) {
    int slot = 0;
    while (slot < getNumRecs() && getDataVal(slot).compareTo(searchkey) < 0)
      slot++;
    return slot - 1;
  }

  public void close() {
    if (currentblk != null)
      tx.unpin(currentblk);
    currentblk = null;
  }

  public boolean isFull() {
    return slotpos(getNumRecs() + 1) >= tx.blockSize();
  }

  /*
   * Split the page at the specified position.
   * flag:
   * 1. the level of directory in BTreeDir
   * 2. the blknum of overflow block in BTreeLeaf
   */
  public BlockId split(int splitpos, int flag) {
    BlockId newblk = appendNew(flag);
    BTPage newpage = new BTPage(tx, newblk, layout);
    transferRecs(splitpos, newpage);
    newpage.setFlag(flag);
    newpage.close();
    return newblk;
  }

  public Constant getDataVal(int slot) {
    return getVal(slot, FLD_NAME_DATAVAL);
  }

  public int getFlag() {
    return tx.getInt(currentblk, 0);
  }

  /*
   * flag stores
   * 1. the level of directory in BTreeDir
   * 2. the blknum of overflow block in BTreeLeaf
   */
  public void setFlag(int val) {
    tx.setInt(currentblk, 0, val, true);
  }

  public BlockId appendNew(int flag) {
    BlockId blk = tx.append(currentblk.fileName());
    tx.pin(blk);
    format(blk, flag);
    return blk;
  }

  public void format(BlockId blk, int flag) {
    tx.setInt(blk, 0, flag, false);
    tx.setInt(blk, Integer.BYTES, 0, false);
    int recsize = layout.slotSize();
    for (int pos = 2 * Integer.BYTES; pos + recsize <= tx.blockSize(); pos += recsize)
      makeDefaultRecord(blk, pos);
  }

  private void makeDefaultRecord(BlockId blk, int pos) {
    for (String fldname : layout.schema().fields()) {
      int offset = layout.offset(fldname);
      if (layout.schema().type(fldname) == INTEGER)
        tx.setInt(blk, pos + offset, 0, false);
      else
        tx.setString(blk, pos + offset, "", false);
    }
  }

  // Methods called only by BTreeDir

  /*
   * Return the block number in the index record
   * at the specified slot
   */
  public int getChildNum(int slot) {
    return getInt(slot, FLD_NAME_BLOCK);
  }

  /*
   * Insert a directory at the specified slot.
   */
  public void insertDir(int slot, Constant val, int blknum) {
    insert(slot);
    setVal(slot, FLD_NAME_DATAVAL, val);
    setInt(slot, FLD_NAME_BLOCK, blknum);
  }

  public RID getDataRid(int slot) {
    return new RID(getInt(slot, FLD_NAME_BLOCK), getInt(slot, FLD_NAME_ID));
  }

  public void insertLeaf(int slot, Constant val, RID rid) {
    insert(slot);
    setVal(slot, FLD_NAME_DATAVAL, val);
    setInt(slot, FLD_NAME_BLOCK, rid.blockNumber());
    setInt(slot, FLD_NAME_ID, rid.slot());
  }

  public void delete(int slot) {
    for (int i = slot + 1; i < getNumRecs(); i++)
      copyRecord(i, i - 1);
    setNumRecs(getNumRecs() - 1);
  }

  /*
   * Return the number of index records in this page.
   */
  public int getNumRecs() {
    return tx.getInt(currentblk, Integer.BYTES);
  }

  private int getInt(int slot, String fldname) {
    int pos = fldpos(slot, fldname);
    return tx.getInt(currentblk, pos);
  }

  private String getString(int slot, String fldname) {
    int pos = fldpos(slot, fldname);
    return tx.getString(currentblk, pos);
  }

  private Constant getVal(int slot, String fldname) {
    int type = layout.schema().type(fldname);
    if (type == INTEGER)
      return new Constant(getInt(slot, fldname));
    else
      return new Constant(getString(slot, fldname));
  }

  private void setInt(int slot, String fldname, int val) {
    int pos = fldpos(slot, fldname);
    tx.setInt(currentblk, pos, val, true);
  }

  private void setString(int slot, String fldname, String val) {
    int pos = fldpos(slot, fldname);
    tx.setString(currentblk, pos, val, true);
  }

  private void setVal(int slot, String fldname, Constant val) {
    int type = layout.schema().type(fldname);
    if (type == INTEGER)
      setInt(slot, fldname, val.asInt());
    else
      setString(slot, fldname, val.asString());
  }

  private void setNumRecs(int n) {
    tx.setInt(currentblk, Integer.BYTES, n, true);
  }

  private void insert(int slot) {
    for (int i = getNumRecs(); i > slot; i--)
      copyRecord(i - 1, i);
    setNumRecs(getNumRecs() + 1);
  }

  private void copyRecord(int from, int to) {
    Schema sch = layout.schema();
    for (String fldname : sch.fields())
      setVal(to, fldname, getVal(from, fldname));
  }

  /*
   * Transfer all the records after the specified slot to the destination BTPage
   */
  private void transferRecs(int slot, BTPage dest) {
    int destslot = 0;
    while (slot < getNumRecs()) {
      dest.insert(destslot);
      Schema sch = layout.schema();
      for (String fldname : sch.fields())
        dest.setVal(destslot, fldname, getVal(slot, fldname));
      delete(slot);
      destslot++;
    }
  }

  private int fldpos(int slot, String fldname) {
    int offset = layout.offset(fldname);
    return slotpos(slot) + offset;
  }

  /*
   * First Integer.BYTES: flag (directory level or overflow blknum)
   * Second Integer.BYTES: number of records
   */
  private int slotpos(int slot) {
    int slotsize = layout.slotSize();
    return Integer.BYTES + Integer.BYTES + (slot * slotsize);
  }
}
