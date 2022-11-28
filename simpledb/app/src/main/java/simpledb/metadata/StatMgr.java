package simpledb.metadata;

import java.util.HashMap;
import java.util.Map;

import simpledb.record.Layout;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

/*
 * Stat manager stores statistical information of each table
 * in tablestats (in memory) not in the database.
 * It calculates the stats on system startup and every 100 retrievals.
 */
public class StatMgr {
  private TableMgr tblMgr;
  // Store stats for each table
  private Map<String, StatInfo> tablestats;
  // used to determine if stats should be updated
  private int numcalls;

  public StatMgr(TableMgr tblMgr, Transaction tx) {
    this.tblMgr = tblMgr;
    refreshStats(tx);
  }

  public synchronized StatInfo getStatInfo(String tblname, Layout layout, Transaction tx) {
    numcalls++;
    if (numcalls > 100)
      refreshStats(tx);

    StatInfo si = tablestats.get(tblname);
    if (si == null) {
      si = calcTableStats(tblname, layout, tx);
      tablestats.put(tblname, si);
    }
    return si;
  }

  private synchronized void refreshStats(Transaction tx) {
    tablestats = new HashMap<>();
    numcalls = 0;
    Layout tcatlayout = tblMgr.getLayout(TableMgr.TBL_CAT_TABLE, tx);
    TableScan tcat = new TableScan(tx, TableMgr.TBL_CAT_TABLE, tcatlayout);
    while (tcat.next()) {
      String tblname = tcat.getString(TableMgr.TBL_CAT_FIELD_TABLE_NAME);
      Layout layout = tblMgr.getLayout(tblname, tx);
      StatInfo si = calcTableStats(tblname, layout, tx);
      tablestats.put(tblname, si);
    }
    tcat.close();
  }

  private synchronized StatInfo calcTableStats(String tblname, Layout layout, Transaction tx) {
    int numRecs = 0;
    int numBlocks = 0;
    TableScan ts = new TableScan(tx, tblname, layout);
    while (ts.next()) {
      numRecs++;
      numBlocks = ts.getRid().blockNumber() + 1;
    }
    ts.close();
    return new StatInfo(numBlocks, numRecs);
  }
}
