package simpledb.metadata;

/*
 * StatInfo stores three pieces of information of a table:
 * 1. the number of blocks
 * 2. the number of records
 * 3. the number of distinct values for each fields (TODO)
 */
public class StatInfo {
  private int numBlocks;
  private int numRecs;

  public StatInfo(int numBlocks, int numRecs) {
    this.numBlocks = numBlocks;
    this.numRecs = numRecs;
  }

  public int blockAccessed() {
    return numBlocks;
  }

  public int recordsOutput() {
    return numRecs;
  }

  /*
   * Return the estimated number of distinct values for the specified fields.
   * Current implementation always returns one thirds of the number of records.
   */
  public int distinctValues(String fldname) {
    // TODO: implement real logic to calculate
    return 1 + (numRecs / 3);
  }
}
