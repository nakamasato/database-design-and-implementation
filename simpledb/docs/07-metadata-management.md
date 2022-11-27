## Chapter 7: Metadata Management

### TableMgr

1. Overview

1. Add `metadata/TableMgr.java`
1. Add the following code to `App.java`

    ```java
    // 7. Metadata Management
    System.out.println("7.1. TableMgr ------------------");
    bm = new BufferMgr(fm, lm, 8); // numbuffs: 3 is not enough
    tx = new Transaction(fm, lm, bm);
    TableMgr tm = new TableMgr(true, tx);
    sch = new Schema();
    sch.addIntField("A");
    sch.addStringField("B", 9);
    tm.createTable("MyTable", sch, tx);

    layout = tm.getLayout("MyTable", tx);
    int size = layout.slotSize();
    Schema sch2 = layout.schema();
    System.out.println("MyTable has slot size" + size);
    System.out.println("Its fields are:");
    for (String fldname : sch2.fields()) {
      String type;
      if (sch2.type(fldname) == INTEGER)
        type = "int";
      else {
        int strlen = sch2.length(fldname);
        type = "varchar(" + strlen + ")";
      }

      System.out.println(fldname + ": " + type);
    }
    tx.commit();
    ```

1. Run
    ```
    ./gradlew run
    ```

    <details>

    ```
    7.1. TableMgr ------------------
    [TableMgr] createTable table: tblcat
    [ConcurrencyMgr] starting new sLock on file: tblcat.tbl, blk: -1. ({})
    [LockTable] starting slock on blk -1
    [LockTable] completed slock on blk -1, status: slock
    [TableScan] moveToBlock file: tblcat.tbl, blk: 0
    [ConcurrencyMgr] starting new sLock on file: tblcat.tbl, blk: 0. ({[file tblcat.tbl, block -1]=S})
    [LockTable] starting slock on blk 0
    [LockTable] completed slock on blk 0, status: slock
    [TableScan] moveToBlock file: tblcat.tbl, blk: 1
    [ConcurrencyMgr] starting new sLock on file: tblcat.tbl, blk: 1. ({[file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S})
    [LockTable] starting slock on blk 1
    [LockTable] completed slock on blk 1, status: slock
    [TableScan] moveToBlock file: tblcat.tbl, blk: 2
    [ConcurrencyMgr] starting new sLock on file: tblcat.tbl, blk: 2. ({[file tblcat.tbl, block 1]=S, [file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S})
    [LockTable] starting slock on blk 2
    [LockTable] completed slock on blk 2, status: slock
    [TableScan] moveToBlock file: tblcat.tbl, blk: 3
    [ConcurrencyMgr] starting new sLock on file: tblcat.tbl, blk: 3. ({[file tblcat.tbl, block 2]=S, [file tblcat.tbl, block 1]=S, [file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S})
    [LockTable] starting slock on blk 3
    [LockTable] completed slock on blk 3, status: slock
    [Transaction] setInt to block 3 with offset 252 and val 1
    [ConcurrencyMgr] starting new xLock on tblcat.tbl, blk: 3. ({[file tblcat.tbl, block 3]=S, [file tblcat.tbl, block 2]=S, [file tblcat.tbl, block 1]=S, [file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S})
    [LockTable] starting xlock on blk: 3
    [LockTable] completed xlock on blk: 3, status: xlock
    [Transaction] setString to block 3 with offset 256 and val tblcat
    [FileMgr] appending block (size 400) to simpledb.log
    [FileMgr] finished appending block. blknum: 27
    [Transaction] setInt to block 3 with offset 276 and val 28
    [ConcurrencyMgr] starting new sLock on file: fldcat.tbl, blk: -1. ({[file tblcat.tbl, block 3]=X, [file tblcat.tbl, block 2]=S, [file tblcat.tbl, block 1]=S, [file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S})
    [LockTable] starting slock on blk -1
    [LockTable] completed slock on blk -1, status: slock
    [TableScan] moveToBlock file: fldcat.tbl, blk: 0
    [ConcurrencyMgr] starting new sLock on file: fldcat.tbl, blk: 0. ({[file tblcat.tbl, block 3]=X, [file tblcat.tbl, block 2]=S, [file tblcat.tbl, block 1]=S, [file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S, [file fldcat.tbl, block -1]=S})
    [LockTable] starting slock on blk 0
    [LockTable] completed slock on blk 0, status: slock
    [Transaction] setInt to block 0 with offset 0 and val 1
    [ConcurrencyMgr] starting new xLock on fldcat.tbl, blk: 0. ({[file tblcat.tbl, block 3]=X, [file tblcat.tbl, block 2]=S, [file fldcat.tbl, block 0]=S, [file tblcat.tbl, block 1]=S, [file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S, [file fldcat.tbl, block -1]=S})
    [LockTable] starting xlock on blk: 0
    [LockTable] completed xlock on blk: 0, status: xlock
    [Transaction] setString to block 0 with offset 4 and val tblcat
    [Transaction] setString to block 0 with offset 24 and val tblname
    [Transaction] setInt to block 0 with offset 44 and val 12
    [Transaction] setInt to block 0 with offset 48 and val 16
    [Transaction] setInt to block 0 with offset 52 and val 4
    [Transaction] setInt to block 0 with offset 56 and val 1
    [Transaction] setString to block 0 with offset 60 and val tblcat
    [Transaction] setString to block 0 with offset 80 and val slotsize
    [FileMgr] appending block (size 400) to simpledb.log
    [FileMgr] finished appending block. blknum: 28
    [Transaction] setInt to block 0 with offset 100 and val 4
    [Transaction] setInt to block 0 with offset 104 and val 0
    [Transaction] setInt to block 0 with offset 108 and val 24
    [TableMgr] createTable completed table: tblcat
    [TableMgr] createTable table: fldcat
    [TableScan] moveToBlock file: tblcat.tbl, blk: 0
    [TableScan] moveToBlock file: tblcat.tbl, blk: 1
    [TableScan] moveToBlock file: tblcat.tbl, blk: 2
    [TableScan] moveToBlock file: tblcat.tbl, blk: 3
    [Transaction] setInt to block 3 with offset 280 and val 1
    [Transaction] setString to block 3 with offset 284 and val fldcat
    [Transaction] setInt to block 3 with offset 304 and val 56
    [TableScan] moveToBlock file: fldcat.tbl, blk: 0
    [Transaction] setInt to block 0 with offset 112 and val 1
    [Transaction] setString to block 0 with offset 116 and val fldcat
    [Transaction] setString to block 0 with offset 136 and val tblname
    [Transaction] setInt to block 0 with offset 156 and val 12
    [FileMgr] appending block (size 400) to simpledb.log
    [FileMgr] finished appending block. blknum: 29
    [Transaction] setInt to block 0 with offset 160 and val 16
    [Transaction] setInt to block 0 with offset 164 and val 4
    [Transaction] setInt to block 0 with offset 168 and val 1
    [Transaction] setString to block 0 with offset 172 and val fldcat
    [Transaction] setString to block 0 with offset 192 and val fldname
    [Transaction] setInt to block 0 with offset 212 and val 12
    [Transaction] setInt to block 0 with offset 216 and val 16
    [Transaction] setInt to block 0 with offset 220 and val 24
    [Transaction] setInt to block 0 with offset 224 and val 1
    [Transaction] setString to block 0 with offset 228 and val fldcat
    [FileMgr] appending block (size 400) to simpledb.log
    [FileMgr] finished appending block. blknum: 30
    [Transaction] setString to block 0 with offset 248 and val type
    [Transaction] setInt to block 0 with offset 268 and val 4
    [Transaction] setInt to block 0 with offset 272 and val 0
    [Transaction] setInt to block 0 with offset 276 and val 44
    [Transaction] setInt to block 0 with offset 280 and val 1
    [Transaction] setString to block 0 with offset 284 and val fldcat
    [Transaction] setString to block 0 with offset 304 and val length
    [Transaction] setInt to block 0 with offset 324 and val 4
    [Transaction] setInt to block 0 with offset 328 and val 0
    [Transaction] setInt to block 0 with offset 332 and val 48
    [FileMgr] appending block (size 400) to simpledb.log
    [FileMgr] finished appending block. blknum: 31
    [Transaction] setInt to block 0 with offset 336 and val 1
    [Transaction] setString to block 0 with offset 340 and val fldcat
    [Transaction] setString to block 0 with offset 360 and val offset
    [Transaction] setInt to block 0 with offset 380 and val 4
    [Transaction] setInt to block 0 with offset 384 and val 0
    [Transaction] setInt to block 0 with offset 388 and val 52
    [TableMgr] createTable completed table: fldcat
    [TableMgr] createTable table: MyTable
    [TableScan] moveToBlock file: tblcat.tbl, blk: 0
    [TableScan] moveToBlock file: tblcat.tbl, blk: 1
    [TableScan] moveToBlock file: tblcat.tbl, blk: 2
    [TableScan] moveToBlock file: tblcat.tbl, blk: 3
    [Transaction] setInt to block 3 with offset 308 and val 1
    [Transaction] setString to block 3 with offset 312 and val MyTable
    [Transaction] setInt to block 3 with offset 332 and val 21
    [TableScan] moveToBlock file: fldcat.tbl, blk: 0
    [TableScan] moveToBlock file: fldcat.tbl, blk: 1
    [ConcurrencyMgr] starting new sLock on file: fldcat.tbl, blk: 1. ({[file tblcat.tbl, block 3]=X, [file tblcat.tbl, block 2]=S, [file fldcat.tbl, block 0]=X, [file tblcat.tbl, block 1]=S, [file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S, [file fldcat.tbl, block -1]=S})
    [LockTable] starting slock on blk 1
    [LockTable] completed slock on blk 1, status: slock
    [Transaction] setInt to block 1 with offset 0 and val 1
    [ConcurrencyMgr] starting new xLock on fldcat.tbl, blk: 1. ({[file tblcat.tbl, block 3]=X, [file tblcat.tbl, block 2]=S, [file fldcat.tbl, block 0]=X, [file fldcat.tbl, block 1]=S, [file tblcat.tbl, block 1]=S, [file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S, [file fldcat.tbl, block -1]=S})
    [LockTable] starting xlock on blk: 1
    [LockTable] completed xlock on blk: 1, status: xlock
    [FileMgr] appending block (size 400) to simpledb.log
    [FileMgr] finished appending block. blknum: 32
    [Transaction] setString to block 1 with offset 4 and val MyTable
    [Transaction] setString to block 1 with offset 24 and val A
    [Transaction] setInt to block 1 with offset 44 and val 4
    [Transaction] setInt to block 1 with offset 48 and val 0
    [Transaction] setInt to block 1 with offset 52 and val 4
    [Transaction] setInt to block 1 with offset 56 and val 1
    [Transaction] setString to block 1 with offset 60 and val MyTable
    [Transaction] setString to block 1 with offset 80 and val B
    [Transaction] setInt to block 1 with offset 100 and val 12
    [Transaction] setInt to block 1 with offset 104 and val 9
    [FileMgr] appending block (size 400) to simpledb.log
    [FileMgr] finished appending block. blknum: 33
    [Transaction] setInt to block 1 with offset 108 and val 8
    [TableMgr] createTable completed table: MyTable
    [TableScan] moveToBlock file: tblcat.tbl, blk: 0
    [TableScan] moveToBlock file: fldcat.tbl, blk: 0
    [TableScan] moveToBlock file: fldcat.tbl, blk: 1
    MyTable has slot size21
    Its fields are:
    A: int
    B: varchar(9)
    transaction 7 committed
    [ConcurrencyMgr] starting release: {[file tblcat.tbl, block 3]=X, [file tblcat.tbl, block 2]=S, [file fldcat.tbl, block 0]=X, [file fldcat.tbl, block 1]=X, [file tblcat.tbl, block 1]=S, [file tblcat.tbl, block 0]=S, [file tblcat.tbl, block -1]=S, [file fldcat.tbl, block -1]=S}
    [LockTable] starting unlock on blk: 3
    [LockTable] completed unlock on blk: 3, status: no lock
    [LockTable] starting unlock on blk: 2
    [LockTable] completed unlock on blk: 2, status: no lock
    [LockTable] starting unlock on blk: 0
    [LockTable] completed unlock on blk: 0, status: no lock
    [LockTable] starting unlock on blk: 1
    [LockTable] completed unlock on blk: 1, status: no lock
    [LockTable] starting unlock on blk: 1
    [LockTable] completed unlock on blk: 1, status: no lock
    [LockTable] starting unlock on blk: 0
    [LockTable] completed unlock on blk: 0, status: no lock
    [LockTable] starting unlock on blk: -1
    [LockTable] completed unlock on blk: -1, status: no lock
    [LockTable] starting unlock on blk: -1
    [LockTable] completed unlock on blk: -1, status: no lock
    [ConcurrencyMgr] completed release: {}
    ```

    </details>

1. Add Test
    ```java
    package simpledb.metadata;

    import static org.junit.jupiter.api.Assertions.assertEquals;
    import static org.junit.jupiter.api.Assertions.assertTrue;

    import java.io.File;

    import org.junit.jupiter.api.Test;

    import simpledb.buffer.BufferMgr;
    import simpledb.file.FileMgr;
    import simpledb.log.LogMgr;
    import simpledb.record.Layout;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    public class TableMgrTest {
      @Test
      public void testCreateTableGetLayout() throws Exception {
        File dbDirectory = new File("datadir");
        FileMgr fm = new FileMgr(dbDirectory, 400);
        LogMgr lm = new LogMgr(fm, "simpledb.log");
        BufferMgr bm = new BufferMgr(fm, lm, 8);
        Transaction tx = new Transaction(fm, lm, bm);
        TableMgr tableMgr = new TableMgr(true, tx);

        Schema sch = new Schema();
        sch.addStringField("name", 50);
        sch.addIntField("count");
        tableMgr.createTable("test_table", sch, tx);

        Layout layout = tableMgr.getLayout("test_table", tx); // read via TableScan (from the file)

        assertEquals(2, layout.schema().fields().size());
        assertEquals(4, layout.offset("name"));
        // 4 bytes for the empty/inuse flag, 54 bytes for name string (4 bytes for the byte
        // length and 50 bytes for bytes themselves)
        assertEquals(4 + 54, layout.offset("count"));
      }
    }
    ```
1. Run test
    ```
    ./gradlew test
    ```

### ViewMgr

1. `metadata/ViewMgr.java`

    ```java
    package simpledb.metadata;

    import simpledb.record.Layout;
    import simpledb.record.Schema;
    import simpledb.record.TableScan;
    import simpledb.tx.Transaction;

    public class ViewMgr {
      private static final int MAX_VIEW_DEF = 100;
      private static final String VIEW_CAT_TABLE = "viewcat";
      private static final String VIEW_CAT_FIELD_NAME = "viewname";
      private static final String VIEW_CAT_FIELD_DEF = "viewdef";

      TableMgr tblMgr;

      public ViewMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
        this.tblMgr = tblMgr;
        if (isNew) {
          Schema sch = new Schema();
          sch.addStringField(VIEW_CAT_FIELD_NAME, TableMgr.MAX_NAME);
          sch.addStringField(VIEW_CAT_FIELD_DEF, MAX_VIEW_DEF);
          tblMgr.createTable(VIEW_CAT_TABLE, sch, tx);
        }
      }

      public void createView(String vname, String vdef, Transaction tx) {
        Layout layout = tblMgr.getLayout(VIEW_CAT_TABLE, tx);
        TableScan ts = new TableScan(tx, VIEW_CAT_TABLE, layout);
        ts.insert();
        ts.setString(VIEW_CAT_FIELD_NAME, vname);
        ts.setString(VIEW_CAT_FIELD_DEF, vdef);
        ts.close();
      }

      public String getViewDef(String vname, Transaction tx) {
        String result = null;
        Layout layout = tblMgr.getLayout(VIEW_CAT_TABLE, tx);
        TableScan ts = new TableScan(tx, VIEW_CAT_TABLE, layout);
        while (ts.next()) {
          if (ts.getString(VIEW_CAT_FIELD_NAME).equals(vname)) {
            result = ts.getString(VIEW_CAT_FIELD_DEF);
            break;
          }
        }
        ts.close();
        return result;
      }
    }
    ```

### StatMgr

1. `metadata/StatInfo.java`

    ```java
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
    ```

1. `metadata/StatMgr.java`

    ```java
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
        Layout tcatlayout = tblMgr.getLayout(tblMgr.TBL_CAT_TABLE, tx);
        TableScan tcat = new TableScan(tx, TableMgr.TBL_CAT_TABLE, tcatlayout);
        while (tcat.next()) {
          String tblname = tcat.getString(tblMgr.TBL_CAT_FIELD_TABLE_NAME);
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
    ```
### IndexMgr

### MetadataMgr