## Chapter 13: Materialization and Sorting

## 13.1. Materialization

1. Add `materialize/TempTable.java`

    ```java
    package simpledb.materialize;

    import simpledb.query.UpdateScan;
    import simpledb.record.Layout;
    import simpledb.record.Schema;
    import simpledb.record.TableScan;
    import simpledb.tx.Transaction;

    public class TempTable {
      private static int nextTableNum = 0;
      private Transaction tx;
      private String tblname;
      private Layout layout;

      public TempTable(Transaction tx, Schema sch) {
        this.tx = tx;
        tblname = nextTableName();
        layout = new Layout(sch);
      }

      public UpdateScan open() {
        return new TableScan(tx, tblname, layout);
      }

      public String tableName() {
        return tblname;
      }

      public Layout getLayout() {
        return layout;
      }

      private static synchronized String nextTableName() {
        nextTableNum++;
        return "temp" + nextTableNum;
      }
    }
    ```

1. Add `materialize/MaterializePlan.java`

    ```java
    package simpledb.materialize;

    import simpledb.plan.Plan;
    import simpledb.query.Scan;
    import simpledb.query.UpdateScan;
    import simpledb.record.Layout;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    public class MaterializePlan implements Plan {
      private Plan srcplan;
      private Transaction tx;

      public MaterializePlan(Transaction tx, Plan srcplan) {
        this.srcplan = srcplan;
        this.tx = tx;
      }

      @Override
      public Scan open() {
        Schema sch = srcplan.schema();
        TempTable temp = new TempTable(tx, sch);
        Scan src = srcplan.open();
        UpdateScan dest = temp.open();
        int cnt = 0;
        while (src.next()) {
          dest.insert();
          for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
          cnt++;
        }
        System.out.println("[MaterializePlan] inserted " + cnt + " records to TempTable[" + temp.tableName() + "]");
        src.close();
        dest.beforeFirst();
        return dest;
      }

      /*
       * Return the estimated number of block access, which
       * doesn't include the one-time cost of materializing the records
       * (preprocessing cost).
       */
      @Override
      public int blockAccessed() {
        Layout layout = new Layout(srcplan.schema());
        double rpb = tx.blockSize() / layout.slotSize();
        return (int) Math.ceil(srcplan.recordsOutput() / rpb);
      }

      @Override
      public int recordsOutput() {
        return srcplan.recordsOutput();
      }

      @Override
      public int distinctValues(String fldname) {
        return srcplan.distinctValues(fldname);
      }

      @Override
      public Schema schema() {
        return srcplan.schema();
      }

    }
    ```

1. Add the following codes to `App.java`

    ```java
    // 13. Materialization and Sorting
    System.out.println("13. Materialization and Sorting -------------");
    tx = new Transaction(fm, lm, bm);
    plan = new TablePlan(tx, "T3", metadataMgr); // metadataMgr created above
    plan = new MaterializePlan(tx, plan);
    us = (UpdateScan) plan.open();
    while (us.next())
      System.out.println("get record from TempTable: " + us.getVal("fld1"));
    ```
1. Run
    ```
    rm -rf app/datadir && ./gradlew run
    ```

    ```
    [MaterializePlan] inserted 2 records to TempTable[temp1]
    [TableScan] moveToBlock file: temp1.tbl, blk: 0
    get record from TempTable: rec0
    get record from TempTable: rec1
    ```

    When opening `MaterializePlan`, all the records in the srcplan will be inserted into the `TempTable`.

## 13.2. Sorting

## 13.3. Grouping and Aggregation

## 13.4. MergeJoin
