package simpledb.tx.recovery;

import simpledb.file.BlockId;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class SetIntRecord implements LogRecord {
  private int txnum;
  private int offset;
  private int val;
  private BlockId blk;

  public SetIntRecord(Page p) {
    int tpos = Integer.BYTES;
    txnum = p.getInt(tpos);
    int fpos = tpos + Integer.BYTES;
    String filename = p.getString(fpos);
    int bpos = fpos + Page.maxLength(filename.length());
    int blknum = p.getInt(bpos);
    blk = new BlockId(filename, blknum);
    int opos = bpos + Integer.BYTES;
    offset = p.getInt(opos);
    int vpos = opos + Integer.BYTES;
    val = p.getInt(vpos);
  }

  public int op() {
    return SETINT;
  }

  public int txNumber() {
    return txnum;
  }

  public String toString() {
    return "<SETINT " + txnum + " " + blk + " " + offset + " " + val + ">";
  }

  public void undo(Transaction tx) {
    tx.pin(blk);
    tx.setInt(blk, offset, val, false);
    tx.unpin(blk);
  }

  public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, int val) {
    int tpos = Integer.BYTES;
    int fpos = tpos + Integer.BYTES;
    int bpos = fpos + Page.maxLength(blk.fileName().length());
    int opos = bpos + Integer.BYTES;
    int vpos = opos + Integer.BYTES;
    byte[] rec = new byte[vpos + Integer.BYTES];
    Page p = new Page(rec);
    p.setInt(0, SETINT);
    p.setInt(tpos, txnum);
    p.setString(fpos, blk.fileName());
    p.setInt(bpos, blk.number());
    p.setInt(opos, offset);
    p.setInt(vpos, val);
    return lm.append(rec);
  }
}
