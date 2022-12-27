package simpledb.index.btree;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

/*
 * B-Tree directory block
 */
public class BTreeDir {
  private Transaction tx;
  private Layout layout;
  private BTPage contents;
  private String filename;

  BTreeDir(Transaction tx, BlockId blk, Layout layout) {
    this.tx = tx;
    this.layout = layout;
    contents = new BTPage(tx, blk, layout);
    filename = blk.fileName();
  }

  public void close() {
    contents.close();
  }

  /*
   * Move to the leaf node for the search key
   */
  public int search(Constant searchkey) {
    BlockId childblk = findChildBlock(searchkey);
    while (contents.getFlag() > 0) { // until reaching the leaf node
      contents.close();
      contents = new BTPage(tx, childblk, layout);
      childblk = findChildBlock(searchkey);
    }
    return childblk.number();
  }

  /*
   * Create a new root block.
   * The new block will have two children:
   * 1. the old root
   * 2. the specified block
   */
  public void makeNewRoot(DirEntry e) {
    Constant firstval = contents.getDataVal(0);
    int level = contents.getFlag();
    BlockId newblk = contents.split(0, level); // transfer all records
    DirEntry oldroot = new DirEntry(firstval, newblk.number());
    insertEntry(oldroot);
    insertEntry(e);
    contents.setFlag(level + 1);
  }

  /*
   * insert a new directory entry into the B-Tree block.
   * 1. If block is at level 0, the entry is just inserted there.
   * 2. Otherwise, the entry is inserted into the leaf node
   * Return the DirEntry if the block split
   */
  public DirEntry insert(DirEntry e) {
    if (contents.getFlag() == 0)
      return insertEntry(e);
    BlockId childblk = findChildBlock(e.dataVal());
    BTreeDir child = new BTreeDir(tx, childblk, layout);
    DirEntry myentry = child.insert(e);
    child.close();
    return (myentry != null) ? insertEntry(myentry) : null;
  }

  /*
   * Insert a new directory entry
   */
  private DirEntry insertEntry(DirEntry e) {
    int newslot = 1 + contents.findSlotBefore(e.dataVal());
    contents.insertDir(newslot, e.dataVal(), e.blockNumber());
    if (!contents.isFull())
      return null;
    int level = contents.getFlag();
    int splitpos = contents.getNumRecs() / 2;
    Constant splitval = contents.getDataVal(splitpos);
    BlockId newblk = contents.split(splitpos, level);
    return new DirEntry(splitval, newblk.number());
  }

  private BlockId findChildBlock(Constant searchkey) {
    int slot = contents.findSlotBefore(searchkey);
    if (contents.getDataVal(slot+1).equals(searchkey))
      slot++;
    int blknum = contents.getChildNum(slot);
    return new BlockId(filename, blknum);
  }
}
