package simpledb.parse;

/*
 * Data for the SQL create index statement
 */
public class CreateIndexData {
  private String idxname;
  private String tblname;
  private String fldname;

  public CreateIndexData(String idxname, String tblname, String fldname) {
    this.idxname = idxname;
    this.tblname = tblname;
    this.fldname = fldname;
  }

  public String indexName() {
    return idxname;
  }

  public String tableName() {
    return tblname;
  }

  public String fieldName() {
    return fldname;
  }
}
