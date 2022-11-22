package simpledb.tx.recovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class RecoveryMgr {
  private LogMgr lm;
  private BufferMgr bm;
  private Transaction tx;
  private int txnum;

  public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm) {
    this.tx = tx;
    this.txnum = txnum;
    this.lm = lm;
    this.bm = bm;
    StartRecord.writeToLog(lm, txnum);
  }

  public void commit() {
    bm.flushAll(txnum);
    int lsn = CommitRecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }

  public void rollback() {
    doRollback();
    bm.flushAll(txnum);
    int lsn = RollbackRecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }

  public void recover() {
    doRecover();
    bm.flushAll(txnum);
    int lsn = CheckpointRecord.writeToLog(lm);
    lm.flush(lsn);
  }

  public int setInt(Buffer buff, int offset) {
    int oldval = buff.contents().getInt(offset);
    BlockId blk = buff.block();
    return SetIntRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }

  public int setString(Buffer buff, int offset) {
    String oldval = buff.contents().getString(offset);
    BlockId blk = buff.block();
    return SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }

  /*
   * iterate through the log records from new to old
   * if it finds log records in the transaction, it calls undo of the log record
   * until the start record of the transaction
   */
  private void doRollback() {
    Iterator<byte[]> iter = lm.iterator();
    while (iter.hasNext()) {
      byte[] bytes = iter.next();
      LogRecord rec = LogRecord.createLogRecord(bytes);
      if (rec.txNumber() == txnum) {
        if (rec.op() == LogRecord.START)
          return;
        rec.undo(tx);
      }
    }
  }

  /*
   * it reads log records until it hits a quiescent checkpoint or reaches the end of the log
   * it undoes uncommited update records
   */
  private void doRecover() {
    Collection<Integer> finishedTxs = new ArrayList<>();
    Iterator<byte[]> iter = lm.iterator();
    while (iter.hasNext()) {
      byte[] bytes = iter.next();
      LogRecord rec = LogRecord.createLogRecord(bytes);
      if (rec.op() == LogRecord.CHECKPOINT)
        return;
      if (rec.op() == LogRecord.COMMIT || rec.op() == LogRecord.ROLLBACK)
        finishedTxs.add(rec.txNumber());
      else if (!finishedTxs.contains(rec.txNumber()))
        rec.undo(tx);
    }
  }
}
