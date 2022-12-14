package simpledb.record;

import static java.sql.Types.INTEGER;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.tx.Transaction;

public class TableScan implements UpdateScan {
  private Transaction tx;
  private Layout layout;
  private RecordPage rp;
  private String filename;
  private int currentslot;

  public TableScan(Transaction tx, String tblname, Layout layout) {
    this.tx = tx;
    this.layout = layout;
    filename = tblname + ".tbl";
    if (tx.size(filename) == 0)
      moveToNewBlock();
    else
      moveToBlock(0);
  }

  // Methods to implement Scan

  @Override
  public void beforeFirst() {
    moveToBlock(0);
  }

  /*
   * return true unless the last block is reached.
   */
  @Override
  public boolean next() {
    currentslot = rp.nextUsedSlot(currentslot);
    while (currentslot < 0) {
      if (atLastBlock())
        return false;
      moveToBlock(rp.block().number() + 1);
      currentslot = rp.nextUsedSlot(currentslot);
    }
    return true;
  }

  @Override
  public int getInt(String fldname) {
    return rp.getInt(currentslot, fldname);
  }

  @Override
  public String getString(String fldname) {
    return rp.getString(currentslot, fldname);
  }

  @Override
  public Constant getVal(String fldname) {
    if (layout.schema().type(fldname) == INTEGER)
      return new Constant(getInt(fldname));
    else
      return new Constant(getString(fldname));
  }

  @Override
  public boolean hasField(String fldname) {
    return layout.schema().hasField(fldname);
  }

  @Override
  public void close() {
    if (rp != null)
      tx.unpin(rp.block());
  }

  @Override
  public void setVal(String fldname, Constant val) {
    if (layout.schema().type(fldname) == INTEGER)
      setInt(fldname, val.asInt());
    else
      setString(fldname, val.asString());
  }

  @Override
  public void setInt(String fldname, int val) {
    rp.setInt(currentslot, fldname, val);
  }

  @Override
  public void setString(String fldname, String val) {
    rp.setString(currentslot, fldname, val);
  }

  @Override
  public void insert() {
    currentslot = rp.useNextEmptySlot(currentslot);
    while (currentslot < 0) {
      if (atLastBlock())
        moveToNewBlock();
      else
        moveToBlock(rp.block().number() + 1);
      currentslot = rp.useNextEmptySlot(currentslot);
    }
  }

  @Override
  public void delete() {
    rp.delete(currentslot);
  }

  @Override
  public RID getRid() {
    return new RID(rp.block().number(), currentslot);
  }

  @Override
  public void moveToRid(RID rid) {
    System.out.println("[TableScan] moveToRid file: " + filename + ", blk: " + rid.blockNumber() + ", slot: " + rid.slot());
    close();
    BlockId blk = new BlockId(filename, rid.blockNumber());
    rp = new RecordPage(tx, blk, layout);
    currentslot = rid.slot();
  }

  // private methods
  private void moveToBlock(int blknum) {
    System.out.println("[TableScan] moveToBlock file: " + filename + ", blk: " + blknum);
    close();
    BlockId blk = new BlockId(filename, blknum);
    rp = new RecordPage(tx, blk, layout);
    currentslot = -1;
  }

  private void moveToNewBlock() {
    close();
    BlockId blk = tx.append(filename);
    rp = new RecordPage(tx, blk, layout);
    rp.format();
    currentslot = -1;
  }

  private boolean atLastBlock() {
    return rp.block().number() == tx.size(filename) - 1;
  }
}
