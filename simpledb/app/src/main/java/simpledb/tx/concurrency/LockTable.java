package simpledb.tx.concurrency;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.BlockId;

public class LockTable {
  private static final long MAX_TIME = 10000; // 10 seconds

  /*
   * The lock state of block:
   * -1 if a transaction holds xlock on the block
   * the number of transactions that hold slock
   */
  private Map<BlockId, Integer> locks = new HashMap<>();

  /*
   * Shared Lock
   * Set the number of transactions that hold shared lock on the block
   */
  public synchronized void sLock(BlockId blk) {
    System.out.println("[LockTable] starting slock on blk " + blk.number());
    try {
      long timestamp = System.currentTimeMillis();
      while (hasXLock(blk) && !waitingTooLong(timestamp))
        wait(MAX_TIME);
      if (hasXLock(blk))
        throw new LockAbortException();

      int val = getLockVal(blk);
      locks.put(blk, val + 1);
    } catch (InterruptedException e) {
      throw new LockAbortException();
    }
    System.out.println("[LockTable] completed slock on blk " + blk.number() + ", status: " + lockStatusName(blk));
  }

  /*
   * Exclusive Lock
   * Set -1 for the block
   */
  synchronized void xLock(BlockId blk) {
    System.out.println("[LockTable] starting xlock on blk: " + blk.number());
    try {
      long timestamp = System.currentTimeMillis();
      while (hasOtherSLocks(blk) && !waitingTooLong(timestamp))
        wait(MAX_TIME);
      if (hasOtherSLocks(blk))
        throw new LockAbortException();
      locks.put(blk, -1);
    } catch (InterruptedException e) {
      throw new LockAbortException();
    }
    System.out.println("[LockTable] completed xlock on blk: " + blk.number() + ", status: " + lockStatusName(blk));
  }

  synchronized void unlock(BlockId blk) {
    System.out.println("[LockTable] starting unlock on blk: " + blk.number());
    int val = getLockVal(blk);
    if (val > 1)
      locks.put(blk, val - 1);
    else {
      locks.remove(blk);
      notifyAll();
    }
    System.out.println("[LockTable] completed unlock on blk: " + blk.number() + ", status: " + lockStatusName(blk));
  }

  private boolean hasXLock(BlockId blk) {
    return getLockVal(blk) < 0; // As it has -1 if a transaction holds xlock
  }

  private boolean hasOtherSLocks(BlockId blk) {
    return getLockVal(blk) > 1; // As it represents the number of transactions holding slock
  }

  private boolean waitingTooLong(long starttime) {
    return System.currentTimeMillis() - starttime > MAX_TIME;
  }

  private int getLockVal(BlockId blk) {
    Integer ival = locks.get(blk);
    int val = (ival == null) ? 0 : ival.intValue();
    return val;
  }

  private String lockStatusName(BlockId blk) {
    Integer ival = locks.get(blk);
    if (ival == null)
      return "no lock";
    else if (ival > 0)
      return "slock";
    else
      return "xlock";
  }
}
