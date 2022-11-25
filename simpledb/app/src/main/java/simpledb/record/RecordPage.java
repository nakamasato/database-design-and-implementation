package simpledb.record;

import static java.sql.Types.INTEGER;

import simpledb.file.BlockId;
import simpledb.tx.Transaction;

public class RecordPage {
  public static final int EMPTY = 0, USED = 1;
  private Transaction tx;
  private BlockId blk;
  private Layout layout;

  public RecordPage(Transaction tx, BlockId blk, Layout layout) {
    this.tx = tx;
    this.blk = blk;
    this.layout = layout;
    tx.pin(blk);
  }

  /*
   * get int from the spesified field at the specified slot.
   * slot is the number to specify a record (n-th record)
   */
  public int getInt(int slot, String fldname) {
    int fldpos = offset(slot) + layout.offset(fldname);
    return tx.getInt(blk, fldpos);
  }

  public String getString(int slot, String fldname) {
    int fldpos = offset(slot) + layout.offset(fldname);
    return tx.getString(blk, fldpos);
  }

  public void setInt(int slot, String fldname, int val) {
    int fldpos = offset(slot) + layout.offset(fldname);
    tx.setInt(blk, fldpos, val, true);
  }

  public void setString(int slot, String fldname, String val) {
    int fldpos = offset(slot) + layout.offset(fldname);
    tx.setString(blk, fldpos, val, true);
  }

  public void delete(int slot) {
    setFlag(slot, EMPTY);
  }

  /*
   * Format all slots in the block with zero-value.
   * These values are not logged as the old values are meaningless.
   */
  public void format() {
    int slot = 0;
    while (isValidSlot(slot)) {
      tx.setInt(blk, offset(slot), EMPTY, false);
      Schema schema = layout.schema();
      for (String fldname : schema.fields()) {
        int fldpos = offset(slot) + layout.offset(fldname);
        if (schema.type(fldname) == INTEGER)
          tx.setInt(blk, fldpos, 0, false);
        else
          tx.setString(blk, fldpos, "", false);
      }
      slot++;
    }
  }

  /*
   * Return the first used slot after the specified slot
   */
  public int nextUsedSlot(int slot) { // nextAfter in the original SimpleDB
    return searchAfter(slot, USED);
  }

  /*
   * Get the next available slot after the specified slot, set the flag to USED,
   * and
   * return the slot.
   * Return -1 if there's no empty slot in the block
   * In SimpleDB, it's implemented as insertAfter, but this method doesn't insert,
   * so renamed to useNextEmptySlot
   */
  public int useNextEmptySlot(int slot) { // insertAfter in the original SimpleDB
    int newslot = searchAfter(slot, EMPTY);
    if (newslot >= 0)
      setFlag(newslot, USED);
    return newslot;
  }

  /*
   * Search for the first slot of the given flag after the given slot
   */
  public int searchAfter(int slot, int flag) {
    slot++;
    while (isValidSlot(slot)) {
      if (tx.getInt(blk, offset(slot)) == flag)
        return slot;
      slot++;
    }
    return -1;
  }

  public BlockId block() {
    return blk;
  }

  /*
   * Set the given flag to the given slot
   */
  public void setFlag(int slot, int flag) {
    tx.setInt(blk, offset(slot), flag, true);
  }

  /*
   * Check if the slot fits in the block of the transaction.
   */
  private boolean isValidSlot(int slot) {
    return offset(slot + 1) <= tx.blockSize();
  }

  private int offset(int slot) {
    return slot * layout.slotSize();
  }
}
