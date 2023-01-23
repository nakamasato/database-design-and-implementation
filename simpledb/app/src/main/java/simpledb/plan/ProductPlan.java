package simpledb.plan;

import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class ProductPlan implements Plan {
  private Plan p1;
  private Plan p2;
  private Schema schema = new Schema();

  public ProductPlan(Plan p1, Plan p2) {
    this.p1 = p1;
    this.p2 = p2;
    schema.addAll(p1.schema());
    schema.addAll(p2.schema());
  }

  @Override
  public Scan open() {
    Scan s1 = p1.open();
    Scan s2 = p2.open();
    return new ProductScan(s1, s2);
  }

  /*
   * Estimate the required block access
   * B(product(p1, p2)) = B(p1) + R(p1)*B(p2)
   */
  @Override
  public int blockAccessed() {
    return p1.blockAccessed() + p1.recordsOutput() * p2.blockAccessed();
  }

  /*
   * Estimate the number of output records
   * R(product(p1, p2)) = R(p1)*R(p2)
   */
  @Override
  public int recordsOutput() {
    return p1.recordsOutput() * p2.recordsOutput();
  }

  /*
   * Estimate the distinct number of field values.
   * The distinct value is same as the underlying query.
   */
  @Override
  public int distinctValues(String fldname) {
    if (p1.schema().hasField(fldname))
      return p1.distinctValues(fldname);
    else
      return p2.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public int preprocessingCost() {
    return 0;
  }
}
