package simpledb.index.btree;

import simpledb.query.Constant;

/*
 * A directory entry has two components:
 * 1. the block number of the child block
 * 2. the dataval of the first record in the block
 */
public class DirEntry {
  private Constant dataval; // dataval of the first record
  private int blocknum; // child block

  public DirEntry(Constant dataval, int blocknum) {
    this.dataval = dataval;
    this.blocknum = blocknum;
  }

  public Constant dataVal() {
    return dataval;
  }

  public int blockNumber() {
    return blocknum;
  }
}
