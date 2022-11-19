package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;

public class BufferMgr {
  private Buffer[] bufferpool;
  private int numAvailable;
  private static final long MAX_TIME = 10000; // 10 seconds

  public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
    bufferpool = new Buffer[numbuffs];
    numAvailable = numbuffs;
    for (int i = 0; i < numbuffs; i++)
      bufferpool[i] = new Buffer(fm, lm);
  }

  public synchronized int available() {
    return numAvailable;
  }

  public synchronized void unpin(Buffer buff) {
    buff.unpin();
    if (!buff.isPinned()) {
      numAvailable++;
      notifyAll();
    }
  }

  public synchronized Buffer pin(BlockId blk) {
    try {
      long timestamp = System.currentTimeMillis();
      Buffer buff = tryToPin(blk);
      while (buff == null && !waitingTooLong(timestamp)) {
        wait(MAX_TIME);
        buff = tryToPin(blk);
      }
      if (buff == null)
        throw new BufferAbortException();
      return buff;
    } catch (InterruptedException e) {
      throw new BufferAbortException();
    }
  }

  private boolean waitingTooLong(long starttime) {
    return System.currentTimeMillis() - starttime > MAX_TIME;
  }

  private Buffer tryToPin(BlockId blk) {
    Buffer buff = findExistingBuffer(blk);
    if (buff == null) {
      buff = chooseUnpinnedBuffer();
      if (buff == null)
        return null;
      buff.assignToBlock(blk);
    }
    if (!buff.isPinned())
      numAvailable--;
    buff.pin();
    return buff;
  }

  private Buffer findExistingBuffer(BlockId blk) {
    for (Buffer buff : bufferpool) {
      BlockId b = buff.block();
      if (b != null && b.equals(blk))
        return buff;
    }
    return null;
  }

  private Buffer chooseUnpinnedBuffer() {
    for (Buffer buff : bufferpool)
      if (!buff.isPinned())
        return buff;
    return null;
  }
}
