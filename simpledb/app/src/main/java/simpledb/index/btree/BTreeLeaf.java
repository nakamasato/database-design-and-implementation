package simpledb.index.btree;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.tx.Transaction;

/*
 * BTreeLeaf stores the contents of a B-Tree leaf block
*/
public class BTreeLeaf {
  private Transaction tx;
  private Layout layout;
  private Constant searchkey;
  private BTPage contents;
  private int currentslot;
  private String filename;

  public BTreeLeaf(Transaction tx, BlockId blk, Layout layout, Constant searchkey) {
    this.tx = tx;
    this.layout = layout;
    this.searchkey = searchkey;
    contents = new BTPage(tx, blk, layout);
    filename = blk.fileName();
  }

  public void close() {
    contents.close();
  }

  public boolean next() {
    currentslot++;
    if (currentslot >= contents.getNumRecs())
      return tryOverflow();
    else if (contents.getDataVal(currentslot).equals(searchkey))
      return true;
    else
      return tryOverflow();
  }

  public RID getDataRid() {
    return contents.getDataRid(currentslot);
  }

  public void delete(RID datarid) {
    while (next())
      if (getDataRid().equals(datarid)) {
        contents.delete(currentslot);
        return;
      }
  }

  /*
   * Insert a new leaf record having the specified dataRID and previously-specified search key.
   */
  public DirEntry insert(RID datarid) {
    // If the new record does not fit in the page, split the page and return directory entry for the new page.
    if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchkey) > 0) {
      Constant firstval = contents.getDataVal(0);
      BlockId newblk = contents.split(0, contents.getFlag());
      currentslot = 0;
      contents.setFlag(-1);
      contents.insertLeaf(currentslot, searchkey, datarid);
      return new DirEntry(firstval, newblk.number());
    }

    currentslot++;
    contents.insertLeaf(currentslot, searchkey, datarid);
    if (!contents.isFull())
      return null;

    // if page is full, split it
    Constant firstkey = contents.getDataVal(0);
    Constant lastkey = contents.getDataVal(contents.getNumRecs() - 1);
    if (lastkey.equals(firstkey)) {
      // create an overflow block to hold all but the first record
      BlockId newblk = contents.split(1, contents.getFlag());
      contents.setFlag(newblk.number());
      return null;
    } else {
      int splitpos = contents.getNumRecs() / 2; // split into half
      Constant splitkey = contents.getDataVal(splitpos);
      if (splitkey.equals(firstkey)) {
        // move right, looking for the next key
        while (contents.getDataVal(splitpos).equals(splitkey))
          splitpos++;
        splitkey = contents.getDataVal(splitpos);
      } else {
        // move left, looking for first entry having that key
        while (contents.getDataVal(splitpos - 1).equals(splitkey))
          splitpos--;
      }
      BlockId newblk = contents.split(splitpos, -1);
      return new DirEntry(splitkey, newblk.number());
    }
  }

  private boolean tryOverflow() {
    Constant firstkey = contents.getDataVal(0);
    int flag = contents.getFlag();
    if (!searchkey.equals(firstkey) || flag < 0)
      return false;
    contents.close();
    BlockId nextblk = new BlockId(filename, flag);
    contents = new BTPage(tx, nextblk, layout);
    currentslot = 0;
    return true;
  }
}
