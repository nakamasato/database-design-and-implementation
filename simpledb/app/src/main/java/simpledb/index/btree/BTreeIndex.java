package simpledb.index.btree;

import static java.sql.Types.INTEGER;

import simpledb.file.BlockId;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class BTreeIndex implements Index {
  private static final String DIR_FLD_BLOCK = "block";
  private static final String DIR_FLD_DATAVAL = "dataval";
  private Transaction tx;
  private Layout dirLayout;
  private Layout leafLayout;
  private String leaftbl;
  private BTreeLeaf leaf = null;
  private BlockId rootblk;

  public BTreeIndex(Transaction tx, String idxname, Layout leafLayout) {
    this.tx = tx;

    // deal with the leaves
    leaftbl = idxname + "leaf";
    this.leafLayout = leafLayout;
    if (tx.size(leaftbl) == 0) {
      BlockId blk = tx.append(leaftbl);
      BTPage node = new BTPage(tx, blk, leafLayout);
      node.format(blk, -1); // -1 means no overflow block
    }

    // deal with the directory
    Schema dirsch = new Schema();
    dirsch.add(DIR_FLD_BLOCK, leafLayout.schema());
    dirsch.add(DIR_FLD_DATAVAL, leafLayout.schema());
    String dirtbl = idxname + "dir";
    dirLayout = new Layout(dirsch);
    rootblk = new BlockId(dirtbl, 0);
    if (tx.size(dirtbl) == 0) {
      // create new root block
      tx.append(dirtbl);
      BTPage node = new BTPage(tx, rootblk, dirLayout);
      node.format(rootblk, 0);
      // insert initial directory entry
      int fldtype = dirsch.type(DIR_FLD_DATAVAL);
      Constant minval = (fldtype == INTEGER) ? new Constant(Integer.MIN_VALUE) : new Constant("");
      node.insertDir(0, minval, 0);
      node.close();
    }
  }

  /*
   * Travere the directory to find the leaf block corresponding to the specified
   * search key.
   * The leaf page is kept open, for use by the methods next and getDataRid.
   */
  @Override
  public void beforeFirst(Constant searchkey) {
    close();
    BTreeDir root = new BTreeDir(tx, rootblk, dirLayout);
    int blknum = root.search(searchkey);
    root.close();
    BlockId leafblk = new BlockId(leaftbl, blknum);
    leaf = new BTreeLeaf(tx, leafblk, leafLayout, searchkey);
  }

  @Override
  public boolean next() {
    return leaf.next();
  }

  @Override
  public RID getDataRid() {
    return leaf.getDataRid();
  }

  /*
   * insert the datarid to the position where the record with the dataval locates.
   * 1. if the leaf is not full, the process just finishes
   * 2. if the leaf splits, add the new node from the root.
   * 3. if the root splits, make a new root.
   */
  @Override
  public void insert(Constant dataval, RID datarid) {
    beforeFirst(dataval);
    DirEntry e = leaf.insert(datarid);
    leaf.close();
    if (e == null) // the leaf wasn't full
      return;

    // if leaf was full and split occurs
    BTreeDir root = new BTreeDir(tx, rootblk, dirLayout);
    DirEntry e2 = root.insert(e);
    if (e2 != null) // if root was split
      root.makeNewRoot(e2);
    root.close();
  }

  /*
   * 1. traverse to find the leaf page containing the record
   * 2. delete the record by datarid
   */
  @Override
  public void delete(Constant dataval, RID datarid) {
    beforeFirst(dataval);
    leaf.delete(datarid);
    leaf.close();
  }

  @Override
  public void close() {
    if (leaf != null)
      leaf.close();
  }

  /*
   * Estimate the number of block accesses
   * required to find all index records haivg a particular search key.
   */
  public static int searchCost(int numblocks, int rpb) {
    return 1 + (int) (Math.log(numblocks) / Math.log(rpb));
  }
}
