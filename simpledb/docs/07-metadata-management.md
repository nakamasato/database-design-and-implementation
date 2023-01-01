## Chapter 7: Metadata Management

### 7.1. TableMgr

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

1. Add mockito to `app/build.gradle.kts`

    ```diff
    dependencies {
    +    // mockito
    +    testImplementation("org.mockito:mockito-core:3.6.0")
    +
    +    // mockito JUnit 5 Extension
    +    testImplementation("org.mockito:mockito-junit-jupiter:3.6.0")
    }
    ```

1. Add Test

    Table Catalog:
    |tablename|slotsize|
    |---|---|
    |tblcat|28|
    |fldcat|56|

    Field Catalog:
    |`tblname`|`fldname`|`type`|`length`|`offset`|
    |---|---|---|---|---|---|
    |tblcat|tblname|1|16|4|
    |tblcat|slotsize|0|0|24|
    |fldcat|tblname|1|16|4|
    |fldcat|fldname|1|16|24|
    |fldcat|type|0|0|44|
    |fldcat|length|0|0|48|
    |fldcat|offset|0|0|52|

    ```java
    package simpledb.metadata;

    import static org.junit.jupiter.api.Assertions.assertEquals;
    import static org.mockito.Mockito.when;

    import java.io.File;

    import org.junit.jupiter.api.Test;
    import org.junit.jupiter.api.extension.ExtendWith;
    import org.mockito.Mock;
    import org.mockito.junit.jupiter.MockitoExtension;

    import simpledb.buffer.BufferMgr;
    import simpledb.file.BlockId;
    import simpledb.file.FileMgr;
    import simpledb.log.LogMgr;
    import simpledb.record.Layout;
    import simpledb.record.RecordPage;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    @ExtendWith(MockitoExtension.class)
    public class TableMgrTest {
      @Mock
      private Transaction tx;

      @Test
      public void testTblCatLayout() throws Exception {
        when(tx.getInt(new BlockId("tblcat.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 0
        when(tx.getString(new BlockId("tblcat.tbl", 0), 4)).thenReturn("tblcat"); // value for record 0
        when(tx.getInt(new BlockId("tblcat.tbl", 0), 24)).thenReturn(28); // flag for record 1
        when(tx.size("tblcat.tbl")).thenReturn(10); // 10 blocks in tbl1.tbl

        when(tx.getInt(new BlockId("fldcat.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 0
        when(tx.getString(new BlockId("fldcat.tbl", 0), 4)).thenReturn("tblcat"); // tblname
        when(tx.getString(new BlockId("fldcat.tbl", 0), 24)).thenReturn("tblname"); // fldname
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 44)).thenReturn(1); // type
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 48)).thenReturn(16); // length
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 52)).thenReturn(4); // offset
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 56)).thenReturn(RecordPage.USED); // flag for record 0
        when(tx.getString(new BlockId("fldcat.tbl", 0), 56 + 4)).thenReturn("tblcat"); // tblname
        when(tx.getString(new BlockId("fldcat.tbl", 0), 56 + 24)).thenReturn("slotsize"); // fldname
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 44)).thenReturn(0); // type
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 48)).thenReturn(0); // length
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 52)).thenReturn(24); // offset
        when(tx.size("fldcat.tbl")).thenReturn(10); // 10 blocks in tbl1.tbl
        when(tx.blockSize()).thenReturn(400);
        TableMgr tableMgr = new TableMgr(false, tx);

        Layout layout = tableMgr.getLayout(TableMgr.TBL_CAT_TABLE, tx);
        assertEquals(4, layout.offset(TableMgr.TBL_CAT_FIELD_TABLE_NAME));
        assertEquals(24, layout.offset(TableMgr.TBL_CAT_FIELD_SLOTSIZE));
        assertEquals(28, layout.slotSize());
      }

      @Test
      public void testFldCatLayout() throws Exception {
        // tblcat.tbl
        when(tx.getInt(new BlockId("tblcat.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 1
        when(tx.getString(new BlockId("tblcat.tbl", 0), 4)).thenReturn("fldcat"); // tblname for record 1
        when(tx.getInt(new BlockId("tblcat.tbl", 0), 24)).thenReturn(56); // fldcat's slotsize
        when(tx.size("tblcat.tbl")).thenReturn(10); // 10 blocks in tbl1.tbl

        // fldcat.tbl
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for fldcat.tblname
        when(tx.getString(new BlockId("fldcat.tbl", 0), 4)).thenReturn("fldcat"); // tblname
        when(tx.getString(new BlockId("fldcat.tbl", 0), 24)).thenReturn("tblname"); // fldname
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 44)).thenReturn(1); // type
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 48)).thenReturn(16); // length
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 52)).thenReturn(4); // offset
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 56)).thenReturn(RecordPage.USED); // flag for fldcat.fldname
        when(tx.getString(new BlockId("fldcat.tbl", 0), 56 + 4)).thenReturn("fldcat"); // tblname
        when(tx.getString(new BlockId("fldcat.tbl", 0), 56 + 24)).thenReturn("fldname"); // fldname
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 44)).thenReturn(1); // type
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 48)).thenReturn(16); // length
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 56 + 52)).thenReturn(24); // offset
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 112)).thenReturn(RecordPage.USED); // flag for fldcat.type
        when(tx.getString(new BlockId("fldcat.tbl", 0), 112 + 4)).thenReturn("fldcat"); // tblname
        when(tx.getString(new BlockId("fldcat.tbl", 0), 112 + 24)).thenReturn("type"); // fldname
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 112 + 44)).thenReturn(0); // type
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 112 + 48)).thenReturn(0); // length
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 112 + 52)).thenReturn(44); // offset
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 168)).thenReturn(RecordPage.USED); // flag for fldcat.length
        when(tx.getString(new BlockId("fldcat.tbl", 0), 168 + 4)).thenReturn("fldcat"); // tblname
        when(tx.getString(new BlockId("fldcat.tbl", 0), 168 + 24)).thenReturn("length"); // fldname
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 168 + 44)).thenReturn(0); // type
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 168 + 48)).thenReturn(0); // length
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 168 + 52)).thenReturn(48); // offset
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 224)).thenReturn(RecordPage.USED); // flag for fldcat.offset
        when(tx.getString(new BlockId("fldcat.tbl", 0), 224 + 4)).thenReturn("fldcat"); // tblname
        when(tx.getString(new BlockId("fldcat.tbl", 0), 224 + 24)).thenReturn("offset"); // fldname
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 224 + 44)).thenReturn(0); // type
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 224 + 48)).thenReturn(0); // length
        when(tx.getInt(new BlockId("fldcat.tbl", 0), 224 + 52)).thenReturn(52); // offset
        when(tx.size("fldcat.tbl")).thenReturn(10); // 10 blocks in tbl1.tbl
        when(tx.blockSize()).thenReturn(400);
        TableMgr tableMgr = new TableMgr(false, tx);

        Layout layout = tableMgr.getLayout(TableMgr.FLD_CAT_TABLE, tx);
        assertEquals(4, layout.offset(TableMgr.FLD_CAT_FIELD_TABLE_NAME));
        assertEquals(24, layout.offset(TableMgr.FLD_CAT_FIELD_FEILD_NAME));
        assertEquals(44, layout.offset(TableMgr.FLD_CAT_FIELD_TYPE));
        assertEquals(48, layout.offset(TableMgr.FLD_CAT_FIELD_LENGTH));
        assertEquals(52, layout.offset(TableMgr.FLD_CAT_FIELD_OFFSET));
        assertEquals(56, layout.slotSize());
      }

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
        // 4 bytes for the empty/inuse flag, 54 bytes for name string (4 bytes for the
        // byte
        // length and 50 bytes for bytes themselves)
        assertEquals(4 + 54, layout.offset("count"));
      }
    }
    ```
1. Run test
    ```
    ./gradlew test
    ```

### 7.2. ViewMgr

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

### 7.3. StatMgr

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
### 7.4. IndexMgr

1. Add `Index` interface `index/Index.java` and `DummyIndex` that implements the interface `index/DummyIndex.java`

    ```java
    package simpledb.index;

    import simpledb.query.Constant;
    import simpledb.record.RID;

    public interface Index {

      /*
      * Positions the index before the first record
      * having the specified search key
      */
      public void beforeFirst(Constant searchkey);

      /*
      * Moves the index to the next record having
      * the search key specified in the beforeFirst method
      */
      public boolean next();

      /*
      * Returns the dataRID value stored in the current index record
      */
      public RID getDataRid();

      /*
      * Inserts an index record having the specified dataval and dataRID values.
      */
      public void insert(Constant dataval, RID datarid);

      /*
      * Deletes the index record having the specified dataval and dataRID values.
      */
      public void delete(Constant dataval, RID datarid);

      /*
      * Closes the index.
      */
      public void close();

    }
    ```

    ```java
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
    ```

1. `metadata/IndexInfo.java`

    ```java
    package simpledb.metadata;

    import static java.sql.Types.INTEGER;

    import simpledb.index.DummyIndex;
    import simpledb.index.Index;
    import simpledb.record.Layout;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    public class IndexInfo {
      private String idxname;
      private String fldname;
      private Transaction tx;
      private Schema tblSchema;
      private Layout idxLayout;
      private StatInfo si;

      public IndexInfo(String idxname, String fldname, Schema tblSchema, Transaction tx, StatInfo si) {
        this.idxname = idxname;
        this.fldname = fldname;
        this.tx = tx;
        this.tblSchema = tblSchema;
        this.idxLayout = createIdxLayout();
        this.si = si;
      }

      public Index open() {
        // TODO: replace with a real Index class
        return new DummyIndex(tx, idxname, idxLayout);
      }

      public int blocksAccessed() {
        int rpb = tx.blockSize() / idxLayout.slotSize();
        int numBlocks = si.recordsOutput() / rpb;
        return DummyIndex.searchCost(numBlocks, rpb);
      }

      /*
      * Return the estimated number of records having a search key.
      * The number of distinct values of the indexed fields.
      * This estimate will be very poor if not evenly distributed.
      */
      public int recordsOutput() {
        return si.recordsOutput() / si.distinctValues(fldname);
      }

      /*
      * Return the distinct values for a specified field or 1 for the indexed field
      */
      public int distinctValues(String fname) {
        return fldname.equals(fname) ? 1 : si.distinctValues(fldname);
      }

      /*
      * Return the layout of the index records.
      * The schema consists of
      * 1. RID (the block number and record id)
      * 2. dataval: the type is determined based on the fldname
      */
      private Layout createIdxLayout() {
        Schema sch = new Schema();
        sch.addIntField("block");
        sch.addIntField("id");
        if (tblSchema.type(fldname) == INTEGER)
          sch.addIntField("dataval");
        else {
          int fldlen = tblSchema.length(fldname);
          sch.addStringField("dataval", fldlen);
        }
        return new Layout(sch);
      }
    }
    ```

1. `metadata/IndexMgr.java`

    ```java
    package simpledb.metadata;

    import static simpledb.metadata.TableMgr.MAX_NAME;

    import java.util.HashMap;
    import java.util.Map;

    import simpledb.record.Layout;
    import simpledb.record.Schema;
    import simpledb.record.TableScan;
    import simpledb.tx.Transaction;

    public class IndexMgr {
      private Layout layout;
      private TableMgr tblmgr;
      private StatMgr statmgr;
      private static final String IDX_CAT_TABLE_NAME = "idxcat";
      private static final String IDX_CAT_FIELD_INDEX_NAME = "indexname";
      private static final String IDX_CAT_FIELD_TABLE_NAME = "tablename";
      private static final String IDX_CAT_FIELD_FIELD_NAME = "fieldname";

      public IndexMgr(boolean isnew, TableMgr tblmgr, StatMgr statmgr, Transaction tx) {
        if (isnew) {
          Schema sch = new Schema();
          sch.addStringField(IDX_CAT_FIELD_INDEX_NAME, MAX_NAME);
          sch.addStringField(IDX_CAT_FIELD_TABLE_NAME, MAX_NAME);
          sch.addStringField(IDX_CAT_FIELD_FIELD_NAME, MAX_NAME);
          tblmgr.createTable(IDX_CAT_TABLE_NAME, sch, tx);
        }
        this.tblmgr = tblmgr;
        this.statmgr = statmgr;
        layout = tblmgr.getLayout(IDX_CAT_TABLE_NAME, tx);
      }

      public void creatIndex(String idxname, String tblname, String fldname, Transaction tx) {
        TableScan ts = new TableScan(tx, IDX_CAT_TABLE_NAME, layout);
        ts.insert();
        ts.setString(IDX_CAT_FIELD_INDEX_NAME, idxname);
        ts.setString(IDX_CAT_FIELD_TABLE_NAME, tblname);
        ts.setString(IDX_CAT_FIELD_FIELD_NAME, fldname);
        ts.close();
      }

      public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
        Map<String, IndexInfo> result = new HashMap<>();
        TableScan ts = new TableScan(tx, IDX_CAT_TABLE_NAME, layout);
        while (ts.next()) {
          if (ts.getString(IDX_CAT_FIELD_TABLE_NAME).equals(tblname)) {
            String idxname = ts.getString(IDX_CAT_FIELD_INDEX_NAME);
            String fldname = ts.getString(IDX_CAT_FIELD_FIELD_NAME);
            Layout tblLayout = tblmgr.getLayout(tblname, tx);
            StatInfo tblsi = statmgr.getStatInfo(tblname, tblLayout, tx);
            IndexInfo ii = new IndexInfo(idxname, fldname, tblLayout.schema(), tx, tblsi);
            result.put(fldname, ii);
          }
        }
        return result;
      }
    }
    ```

### 7.5. MetadataMgr

1. Add `metadata/MetadataMgr.java`

    ```java
    package simpledb.metadata;

    import java.util.Map;

    import simpledb.record.Layout;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    /*
    * Metadata Manager holds the four managers
    * 1. table manager
    * 2. view manager
    * 3. stat manager
    * 4. index manager
    */
    public class MetadataMgr {
      private static TableMgr tblmgr;
      private static ViewMgr viewmgr;
      private static StatMgr statmgr;
      private static IndexMgr idxmgr;

      public MetadataMgr(boolean isnew, Transaction tx) {
        tblmgr = new TableMgr(isnew, tx);
        viewmgr = new ViewMgr(isnew, tblmgr, tx);
        statmgr = new StatMgr(tblmgr, tx);
        idxmgr = new IndexMgr(isnew, tblmgr, statmgr, tx);
      }

      public void createTable(String tblname, Schema sch, Transaction tx) {
        tblmgr.createTable(tblname, sch, tx);
      }

      public Layout getLayout(String tblname, Transaction tx) {
        return tblmgr.getLayout(tblname, tx);
      }

      public void createView(String viewname, String viewdef, Transaction tx) {
        viewmgr.createView(viewname, viewdef, tx);
      }

      public String getViewDef(String viewname, Transaction tx) {
        return viewmgr.getViewDef(viewname, tx);
      }

      public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
        idxmgr.creatIndex(idxname, tblname, fldname, tx);
      }

      public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
        return idxmgr.getIndexInfo(tblname, tx);
      }

      public StatInfo getStatInfo(String tblname, Layout layout, Transaction tx) {
        return statmgr.getStatInfo(tblname, layout, tx);
      }
    }
    ```

1. Add the following code to App.java

    ```java
    System.out.println("7.5. MetadataMgr ----------------");
    tx = new Transaction(fm, lm, bm);
    MetadataMgr metadataMgr = new MetadataMgr(true, tx);
    sch = new Schema();
    sch.addStringField("name", 50);
    sch.addIntField("count");

    // Create Table
    metadataMgr.createTable("test_table", sch, tx);

    layout = metadataMgr.getLayout("test_table", tx); // read via TableScan (from the file)
    System.out.println("layout.schema.fields.size (expected: 2): " + layout.schema().fields().size());
    System.out.println("layout.offset for name (expected: 4): " + layout.offset("name"));
    System.out.println("layout.offset for name (expected: 54): " + layout.offset("count"));

    metadataMgr.createView("test_view", "view def", tx);
    String viewdef = metadataMgr.getViewDef("test_view", tx); // read via TableScan (from the file)
    System.out.println("view def: " + viewdef);
    tx.commit();
    ```
1. ToDO: test with test codes

    ```java
    package simpledb.metadata;

    import static org.junit.jupiter.api.Assertions.assertEquals;

    import java.io.File;
    import java.util.Map;

    import org.junit.jupiter.api.Test;

    import simpledb.buffer.BufferMgr;
    import simpledb.file.FileMgr;
    import simpledb.log.LogMgr;
    import simpledb.record.Layout;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    public class MetadataMgrTest {
      @Test
      public void testMetadataMgrTest() throws Exception {
        File dbDirectory = new File("datadir");
        FileMgr fm = new FileMgr(dbDirectory, 400);
        LogMgr lm = new LogMgr(fm, "simpledb.log");
        BufferMgr bm = new BufferMgr(fm, lm, 8);
        Transaction tx = new Transaction(fm, lm, bm);
        MetadataMgr metadataMgr = new MetadataMgr(true, tx);

        Schema sch = new Schema();
        sch.addStringField("name", 50);
        sch.addIntField("count");

        // Create Table
        metadataMgr.createTable("test_table", sch, tx);

        Layout layout = metadataMgr.getLayout("test_table", tx); // read via TableScan (from the file)
        assertEquals(2, layout.schema().fields().size());
        assertEquals(4, layout.offset("name"));
        // 4 bytes for the empty/inuse flag, 54 bytes for name string (4 bytes for the
        // byte
        // length and 50 bytes for bytes themselves)
        //
        assertEquals(4 + 54, layout.offset("count"));

        // Create View
        metadataMgr.createView("test_view", "view def", tx);

        String viewdef = metadataMgr.getViewDef("test_view", tx); // read via TableScan (from the file)
        assertEquals("view def", viewdef);
      }
    }
    ```
