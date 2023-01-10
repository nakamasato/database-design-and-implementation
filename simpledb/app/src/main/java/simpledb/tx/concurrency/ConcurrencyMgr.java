package simpledb.tx.concurrency;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.BlockId;

/*
 * Concurrency Manager implements lock protocol using block-level granularity.
 * and created for a transaction. The same lock table needs to be shared.
 */
public class ConcurrencyMgr {
  /*
   * The lock table is shared among all Concurrency Manager as it's a static
   * variable
   */
  private static LockTable locktbl = new LockTable();
  /*
   * The lock state of block:
   * S if THE transaction holds slock on the block
   * X if THE transaction holds xlock on the block
   */
  private Map<BlockId, String> locks = new HashMap<>();

  /*
   * Shared Lock
   */
  public void sLock(BlockId blk) {
    locks.computeIfAbsent(blk, k -> {
      System.out
          .println("[ConcurrencyMgr] starting new sLock on file: " + blk.fileName() + ", blk: " + blk.number() + ". ("
              + toString() + ")");
      locktbl.sLock(k);
      return "S";
    });
  }

  /*
   * Exclusive Lock
   * If the block doesn't have xlock, firstly get sLock and them promote to xlock
   */
  public void xLock(BlockId blk) {
    if (!hasXLock(blk)) {
      System.out.println("[ConcurrencyMgr] starting new xLock on " + blk.fileName() + ", blk: " + blk.number() + ". ("
          + toString() + ")");
      sLock(blk);
      locktbl.xLock(blk);
      locks.put(blk, "X");
    }
  }

  /*
   * Release all locks
   */
  public void release() {
    for (BlockId blk : locks.keySet())
      locktbl.unlock(blk);
    locks.clear();
    System.out.println("[ConcurrencyMgr] completed release: " + toString());
  }

  private boolean hasXLock(BlockId blk) {
    String locktype = locks.get(blk);
    return locktype != null && locktype.equals("X");
  }

  public String toString() {
    return locks.toString();
  }
}
