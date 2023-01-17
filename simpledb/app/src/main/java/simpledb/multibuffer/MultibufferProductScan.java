package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

/*
 * RHS is scanned with ChunkScan.
 */
public class MultibufferProductScan implements Scan {
  private Transaction tx;
  private Scan lhsscan;
  private Scan rhsscan = null;
  private Scan prodscan;
  private String filename;
  private Layout layout;
  private int chunksize; // number of blocks processed together
  private int nextblknum;
  private int filesize;

  public MultibufferProductScan(Transaction tx, Scan lhsscan, String tblname, Layout layout) {
    this.tx = tx;
    this.lhsscan = lhsscan;
    this.filename = tblname + ".tbl";
    this.layout = layout;
    filesize = tx.size(filename);
    int available = tx.availableBuffs();
    chunksize = BufferNeeds.bestFactor(available, filesize);
    beforeFirst();
  }

  /*
   * LHS scan is positioned at its first record.
   * RHS scan is positioned before the first records of the first record.
   */
  @Override
  public void beforeFirst() {
    nextblknum = 0;
    useNextChunk();
  }

  /*
   * Move to next chunk until there's no more chunks.
   * Repeat it until ProductScan has no more records.
   */
  @Override
  public boolean next() {
    while (!prodscan.next())
      if (!useNextChunk())
        return false;
    return true;
  }

  @Override
  public int getInt(String fldname) {
    return prodscan.getInt(fldname);
  }

  @Override
  public String getString(String fldname) {
    return prodscan.getString(fldname);
  }

  @Override
  public Constant getVal(String fldname) {
    return prodscan.getVal(fldname);
  }

  @Override
  public boolean hasField(String fldname) {
    return prodscan.hasField(fldname);
  }

  @Override
  public void close() {
    prodscan.close();
  }

  /*
   * Create ChunkScan for star and end block num.
   * Move LHS before the first record.
   */
  private boolean useNextChunk() {
    int startblknum = nextblknum;
    if (startblknum >= filesize)
      return false;
    if (rhsscan != null)
      rhsscan.close();
    int endblknum = startblknum + chunksize - 1;
    if (endblknum >= filesize)
      endblknum = filesize - 1;
    rhsscan = new ChunkScan(tx, filename, layout, startblknum, endblknum);
    lhsscan.beforeFirst();
    prodscan = new ProductScan(lhsscan, rhsscan);
    nextblknum = endblknum + 1;
    return true;
  }
}
