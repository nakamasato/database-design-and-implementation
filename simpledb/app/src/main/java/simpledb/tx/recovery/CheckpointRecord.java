package simpledb.tx.recovery;

import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class CheckpointRecord implements LogRecord {

  public int op() {
    return CHECKPOINT;
  }

  public int txNumber() {
    return -1;
  }

  public void undo(Transaction tx) {
    // Do nothing. because a checkpoint record
    // contains no undo information.
  }

  public String toString() {
    return "<CHECKPOINT>";
  }

  public static int writeToLog(LogMgr lm) {
    byte[] rec = new byte[Integer.BYTES];
    Page p = new Page(rec);
    p.setInt(0, CHECKPOINT);
    return lm.append(rec);
  }
}
