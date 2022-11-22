package simpledb.tx.recovery;

import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class CommitRecord implements LogRecord {
  private int txnum;

  public CommitRecord(Page p) {
    int tpos = Integer.BYTES;
    txnum = p.getInt(tpos);
  }

  public int op() {
    return COMMIT;
  }

  public int txNumber() {
    return txnum;
  }

  public void undo(Transaction tx) {
    // Do nothing. because a commit record
    // contains no undo information.
  }

  public String toString() {
    return "<COMMIT " + txnum + ">";
  }

  public static int writeToLog(LogMgr lm, int txnum) {
    byte[] rec = new byte[2 * Integer.BYTES];
    Page p = new Page(rec);
    p.setInt(0, COMMIT);
    p.setInt(Integer.BYTES, txnum);
    return lm.append(rec);
  }
}
