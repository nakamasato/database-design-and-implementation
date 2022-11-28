package simpledb.index;

import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.tx.Transaction;

public class DummyIndex implements Index {

  public DummyIndex(Transaction tx, String idxname, Layout idxLayout) {
  }

  public static int searchCost(int numBlocks, int rpb) {
    // TODO: implement later
    return 1;
  }

  @Override
  public void beforeFirst(Constant searchkey) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean next() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public RID getDataRid() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void insert(Constant dataval, RID datarid) {
    // TODO Auto-generated method stub

  }

  @Override
  public void delete(Constant dataval, RID datarid) {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

}
