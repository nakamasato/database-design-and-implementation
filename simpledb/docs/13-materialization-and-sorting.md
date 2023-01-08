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
    System.out.println("13.1. Materialization --------");
    tx = new Transaction(fm, lm, bm);
    plan = new TablePlan(tx, "T3", metadataMgr); // metadataMgr created above
    plan = new MaterializePlan(tx, plan);
    Scan scan = plan.open();
    while (scan.next())
      System.out.println("get record from TempTable: " + scan.getVal("fld1"));

    scan.close();
    tx.commit();
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

1. Add `materialize/RecordComparator.java`

    ```java
    package simpledb.materialize;

    import java.util.Comparator;
    import java.util.List;

    import simpledb.query.Constant;
    import simpledb.query.Scan;

    /*
     * A comparator for Scans with the specified fields
     */
    public class RecordComparator implements Comparator<Scan> {
      private List<String> fields;

      public RecordComparator(List<String> fields) {
        this.fields = fields;
      }

      /*
       * Compare the current records of the two specified scans
       */
      @Override
      public int compare(Scan s1, Scan s2) {
        for (String fldname : fields) {
          Constant val1 = s1.getVal(fldname);
          Constant val2 = s2.getVal(fldname);
          int result = val1.compareTo(val2);
          if (result != 0)
            return result;
        }
        return 0;
      }
    }
    ```

1. Add `materialize/SortPlan.java`

    ```java
    package simpledb.materialize;

    import java.util.ArrayList;
    import java.util.List;

    import simpledb.plan.Plan;
    import simpledb.query.Scan;
    import simpledb.query.UpdateScan;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    public class SortPlan implements Plan {
      private Transaction tx;
      private Plan p;
      private Schema sch;
      private RecordComparator comp;

      public SortPlan(Transaction tx, Plan p, List<String> sortfields) {
        this.tx = tx;
        this.p = p;
        sch = p.schema();
        comp = new RecordComparator(sortfields);
      }

      /*
       * Do mergesort before returning Scan
       */
      @Override
      public Scan open() {
        Scan src = p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        while (runs.size() > 2)
          runs = doAMergeIteration(runs);
        return new SortScan(runs, comp);
      }

      @Override
      public int blockAccessed() {
        Plan mp = new MaterializePlan(tx, p);
        return mp.blockAccessed();
      }

      @Override
      public int recordsOutput() {
        return p.recordsOutput();
      }

      @Override
      public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
      }

      @Override
      public Schema schema() {
        return sch;
      }

      /*
       * split into TempTables in that all records in each TempTable
       * are sorted.
       */
      private List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<>();
        src.beforeFirst();
        if (!src.next())
          return temps;
        TempTable currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan))
          if (comp.compare(src, currentscan) < 0) {
            currentscan.close();
            currenttemp = new TempTable(tx, sch);
            temps.add(currenttemp);
            currentscan = currenttemp.open();
          }
        currentscan.close();
        return temps;
      }

      private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() > 1) {
          TempTable p1 = runs.remove(0);
          TempTable p2 = runs.remove(0);
          result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1)
          result.add(runs.get(0));
        return result;
      }

      /*
       * Merge two runs:
       * 1. Create a new TempTable (to be merged table)
       * 2. Insert the smaller record into the destination table
       * until no more records exists.
       */
      private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();
        while (hasmore1 && hasmore2)
          if (comp.compare(src1, src2) < 0)
            hasmore1 = copy(src1, dest);
          else
            hasmore2 = copy(src2, dest);

        if (hasmore1)
          while (hasmore1)
            hasmore1 = copy(src1, dest);
        else
          while (hasmore2)
            hasmore2 = copy(src2, dest);
        src1.close();
        src2.close();
        return result;
      }

      private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : sch.fields())
          dest.setVal(fldname, src.getVal(fldname));
        return src.next();
      }
    }
    ```

1. Add `materialize/SortScan.java`

    ```java
    package simpledb.materialize;

    import java.util.List;

    import simpledb.query.Constant;
    import simpledb.query.Scan;
    import simpledb.query.UpdateScan;

    /*
     * Create a sort scan, given a list of 1 or 2 runs.
     * If there is only 1 run, then s2 will be null and
     * hasmore2 will be false.
     */
    public class SortScan implements Scan {
      private UpdateScan s1;
      private UpdateScan s2 = null;
      private UpdateScan currentscan = null;
      private RecordComparator comp;
      private boolean hasmore1;
      private boolean hasmore2 = false;

      public SortScan(List<TempTable> runs, RecordComparator comp) {
        this.comp = comp;
        s1 = runs.get(0).open();
        hasmore1 = s1.next();
        if (runs.size() > 1) {
          s2 = runs.get(1).open();
          hasmore2 = s2.next();
        }
      }

      /*
       * Position the scan before the first record in sorted order.
       * Internally, it moves to the first record of each underlying scan.
       * The variable currentscan is set to null, indicating that there is
       * no current scan.
       */
      @Override
      public void beforeFirst() {
        currentscan = null;
        s1.beforeFirst();
        hasmore1 = s1.next();
        if (s2 != null) {
          s2.beforeFirst();
          hasmore2 = s2.next();
        }
      }

      /*
       * Increment currentscan
       * Set currentscan after comparing s1 and s2
       */
      @Override
      public boolean next() {
        if (currentscan != null) {
          if (currentscan == s1)
            hasmore1 = s1.next();
          else if (currentscan == s2)
            hasmore2 = s2.next();
        }

        if (!hasmore1 && !hasmore2) // false & false
          return false;
        else if (hasmore1 && hasmore2) { // true & true
          if (comp.compare(s1, s2) < 0)
            currentscan = s1;
          else
            currentscan = s2;
        } else if (hasmore1) // true & false
          currentscan = s1;
        else // false & true
          currentscan = s2;
        return true;
      }

      @Override
      public int getInt(String fldname) {
        return currentscan.getInt(fldname);
      }

      @Override
      public String getString(String fldname) {
        return currentscan.getString(fldname);
      }

      @Override
      public Constant getVal(String fldname) {
        return currentscan.getVal(fldname);
      }

      @Override
      public boolean hasField(String fldname) {
        return currentscan.hasField(fldname);
      }

      @Override
      public void close() {
        s1.close();
        if (s2 != null)
          s2.close();
      }
    }
    ```

1. Add the following code to `App.java`

    ```java
    System.out.println("13.2. Sorting --------------------");
    tx = new Transaction(fm, lm, bm);
    plan = new TablePlan(tx, "T3", metadataMgr);
    plan = new SortPlan(tx, plan, Arrays.asList("fld1"));
    scan = plan.open();
    while (scan.next())
      System.out.println("get record from sorted TempTable: " + scan.getVal("fld1"));

    scan.close();
    tx.commit();
    ```

1. Run
    ```
    rm -rf app/datadir && ./gradlew run
    ```

    ```
    [SortPlan] split into 1 runs
    [SortPlan] merged into 1 runs
    [TableScan] moveToBlock file: temp2.tbl, blk: 0
    get record from sorted TempTable: rec0
    get record from sorted TempTable: rec1
    ```

    In this example, it's too few records to split into multiple runs.

## 13.3. Grouping and Aggregation

## 13.4. MergeJoin
