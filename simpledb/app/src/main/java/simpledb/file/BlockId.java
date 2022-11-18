package simpledb.file;

public class BlockId {
  private String filename;
  private int blknum;

  public BlockId(String filename, int blknum) {
    this.filename = filename;
    this.blknum = blknum;
  }

  public String fileName() {
    return filename;
  }

  public int number() {
    return blknum;
  }

  public boolean equals(Object obj) {
    BlockId blk = (BlockId) obj;
    if (blk == null)
      return false;
    return filename.equals(blk.fileName()) && blknum == blk.number();
  }

  public String toString() {
    return "[file " + filename + ", block " + blknum + "]";
  }

  public int hashCode() {
    return toString().hashCode();
  }
}
