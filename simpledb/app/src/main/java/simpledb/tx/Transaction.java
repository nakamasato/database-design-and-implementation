package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.concurrency.ConcurrencyMgr;
import simpledb.tx.recovery.RecoveryMgr;

public class Transaction {
  private static int nextTxNum = 0;
  private static final int END_OF_FILE = -1;
  private RecoveryMgr recoveryMgr;
  private ConcurrencyMgr concurMgr;
  private BufferMgr bm;
  private FileMgr fm;
  private int txnum;
  private BufferList mybuffers;

  public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
    this.fm = fm;
    this.bm = bm;
    txnum = nextTxNumber();
    recoveryMgr = new RecoveryMgr(this, txnum, lm, bm);
    concurMgr = new ConcurrencyMgr();
    mybuffers = new BufferList(bm);
  }

  public void commit() {
    recoveryMgr.commit();
    System.out.println("transaction " + txnum + " committed");
    concurMgr.release();
    mybuffers.unpinAll();
  }

  public void rollback() {
    recoveryMgr.rollback();
    System.out.println("transaction " + txnum + " rolled back");
    concurMgr.release();
    mybuffers.unpinAll();
  }

  public void recover() {
    bm.flushAll(txnum);
    recoveryMgr.recover();
  }

  public void pin(BlockId blk) {
    mybuffers.pin(blk);
  }

  public void unpin(BlockId blk) {
    mybuffers.unpin(blk);
  }

  public int getInt(BlockId blk, int offset) {
    concurMgr.sLock(blk);
    Buffer buff = mybuffers.getBuffer(blk);
    return buff.contents().getInt(offset);
  }

  public String getString(BlockId blk, int offset) {
    concurMgr.sLock(blk);
    Buffer buff = mybuffers.getBuffer(blk);
    return buff.contents().getString(offset);
  }

  public void setInt(BlockId blk, int offset, int val, boolean okToLog) {
    concurMgr.xLock(blk);
    Buffer buff = mybuffers.getBuffer(blk);
    int lsn = -1;
    if (okToLog)
      lsn = recoveryMgr.setInt(buff, offset);
    Page p = buff.contents();
    p.setInt(offset, val);
    buff.setModified(txnum, lsn);
  }

  public void setString(BlockId blk, int offset, String val, boolean okToLog) {
    concurMgr.xLock(blk);
    Buffer buff = mybuffers.getBuffer(blk);
    int lsn = -1;
    if (okToLog)
      lsn = recoveryMgr.setString(buff, offset);

    Page p = buff.contents();
    p.setString(offset, val);
    buff.setModified(txnum, lsn);
  }

  /*
   * Append a new block to the specified file.
   * Get xlock for END_OF_FILE before appending
   */
  public BlockId append(String filename) {
    BlockId dummyblk = new BlockId(filename, END_OF_FILE);
    concurMgr.xLock(dummyblk);
    return fm.append(filename);
  }

  public int blockSize() {
    return fm.blockSize();
  }

  private static synchronized int nextTxNumber() {
    nextTxNum++;
    return nextTxNum;
  }

  /*
   * Return the number of blocks in the specified file.
   * The method first obtains Slock on the "end of file"
   * before asking the file manager to return the file size
   */
  public int size(String filename) {
    BlockId dummyblk = new BlockId(filename, END_OF_FILE);
    concurMgr.sLock(dummyblk);
    return fm.length(filename);
  }
}
