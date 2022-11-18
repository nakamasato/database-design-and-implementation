package simpledb.log;

import java.util.Iterator;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

public class LogIterator implements Iterator<byte[]> {
  private FileMgr fm;
  private BlockId blk;
  private Page p;
  private int currentpos;
  private int boundary; // what is the boundary?

  public LogIterator(FileMgr fm, BlockId blk) {
    this.fm = fm;
    this.blk = blk;
    byte[] b = new byte[fm.blockSize()];
    p = new Page(b);
    moveToBlock(blk);
  }

  public boolean hasNext() {
    return currentpos < fm.blockSize() || blk.number() > 0;
  }

  /*
   * Read logs from new to old (New Block to old block)
   * Inside a block, contents will be read from left to right.
   * The logs are written from right to left, so the reading order is descendent.
   */
  public byte[] next() {
    if (currentpos == fm.blockSize()) {
      blk = new BlockId(blk.fileName(), blk.number() - 1); // decrement block number to move to next block
      moveToBlock(blk);
    }
    byte[] rec = p.getBytes(currentpos);
    currentpos += Integer.BYTES + rec.length;
    return rec;
  }

  /*
   * Read block contents to the page
   * Set the boundary to the number stored in the first four bytes
   * which indicates the boundary.
   * Set the current position to the obtained boundary.
   */
  private void moveToBlock(BlockId blk) {
    fm.read(blk, p);
    boundary = p.getInt(0);
    currentpos = boundary;
  }
}
