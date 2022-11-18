package simpledb.log;

import java.util.Iterator;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

public class LogMgr {
  private FileMgr fm;
  private String logfile;
  private Page logpage;
  private BlockId currentblk;
  private int latestLSN = 0;
  private int lastSavedLSN = 0;

  public LogMgr(FileMgr fm, String logfile) {
    this.fm = fm;
    this.logfile = logfile;
    byte[] b = new byte[fm.blockSize()];
    logpage = new Page(b);
    int logsize = fm.length(logfile);
    if (logsize == 0) {
      currentblk = appendNewBlock();
    } else {
      currentblk = new BlockId(logfile, logsize - 1);
      fm.read(currentblk, logpage);
    }
  }

  public void flush(int lsn) {
    if (lsn >= lastSavedLSN)
      flush();
  }

  public Iterator<byte[]> iterator() {
    flush(); // why flush here?
    return new LogIterator(fm, currentblk);
  }

  public synchronized int append(byte[] logrec) {
    int boundary = logpage.getInt(0);
    int recsize = logrec.length;
    int bytesneeded = recsize + Integer.BYTES;
    if (boundary - bytesneeded < Integer.BYTES) {
      flush();
      currentblk = appendNewBlock();
      boundary = logpage.getInt(0);
    }
    int recpos = boundary - bytesneeded;

    logpage.setBytes(recpos, logrec);
    logpage.setInt(0, recpos);
    latestLSN += 1;
    return latestLSN;
  }

  private BlockId appendNewBlock() {
    BlockId blk = fm.append(logfile);
    logpage.setInt(0, fm.blockSize());
    fm.write(blk, logpage);
    return blk;
  }

  private void flush() {
    fm.write(currentblk, logpage);
    lastSavedLSN = latestLSN;
  }
}
