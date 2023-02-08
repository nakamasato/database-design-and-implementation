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
    plan = new TablePlan(tx, "T1", metadataMgr);
    plan = new SortPlan(tx, plan, Arrays.asList("A"));
    scan = plan.open();
    while (scan.next())
      System.out.println("get record from sorted TempTable: " + scan.getVal("A"));

    scan.close();
    tx.commit();
    ```

1. Run
    ```
    rm -rf app/datadir && ./gradlew run
    ```

    ```
    [SortPlan] merged into 2 runs
    [TableScan] moveToBlock file: temp9.tbl, blk: 0
    [TableScan] moveToBlock file: temp6.tbl, blk: 0
    get record from sorted TempTable: 10
    get record from sorted TempTable: 11
    get record from sorted TempTable: 11
    get record from sorted TempTable: 22
    get record from sorted TempTable: 24
    get record from sorted TempTable: 28
    get record from sorted TempTable: 32
    get record from sorted TempTable: 32
    get record from sorted TempTable: 40
    get record from sorted TempTable: 40
    ```

    You may see different result as the data in `T1` is random but you can see the records are sorted.

## 13.3. Grouping and Aggregation
1. Add `materialize/AggregationFn.java`
    ```java
    package simpledb.materialize;

    import simpledb.query.Constant;
    import simpledb.query.Scan;

    /*
     * Aggregation function used by groupby operator
     */
    public interface AggregationFn {
      void processFirst(Scan s);

      void processNext(Scan s);

      String fieldName();

      Constant value();
    }
    ```
1. Add `materialize/GroupValue.java`
    `GroupValue` stores the field-value pairs for the grouping fields for a record.
    ```java
    package simpledb.materialize;

    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;

    import simpledb.query.Constant;
    import simpledb.query.Scan;

    /*
     * Object to hold the values of the grouping fields for
     * the current record of a scan.
     */
    public class GroupValue {
      private Map<String, Constant> vals = new HashMap<>();

      public GroupValue(Scan s, List<String> fields) {
        vals = new HashMap<>();
        for (String fldname : fields)
          vals.put(fldname, s.getVal(fldname));
      }

      public Constant getVal(String fldname) {
        return vals.get(fldname);
      }

      public boolean equals(Object obj) {
        if (obj == null)
          return false;
        GroupValue gv = (GroupValue) obj;
        for (Map.Entry<String, Constant> e : vals.entrySet()) {
          Constant v1 = e.getValue();
          Constant v2 = gv.getVal(e.getKey());
          if (!v1.equals(v2))
            return false;
        }
        return true;
      }

      /*
       * The hashcode of a GroupValue object is
       * the sum of the hashcodes of its field values.
       */
      public int hashCode() {
        int hashval = 0;
        for (Constant c : vals.values())
          hashval += c.hashCode();
        return hashval;
      }
    }
    ```
1. Add `materialize/GroupByScan.java`
    ```java
    package simpledb.materialize;

    import java.util.List;

    import simpledb.query.Constant;
    import simpledb.query.Scan;

    public class GroupByScan implements Scan {
      private Scan s;
      private List<String> groupfields;
      private List<AggregationFn> aggfns;
      private GroupValue groupval;
      private boolean moregroups; // boolean to indicates if the underlying scan has records to read

      public GroupByScan(Scan s, List<String> groupfields, List<AggregationFn> aggfns) {
        this.s = s;
        this.groupfields = groupfields;
        this.aggfns = aggfns;
        beforeFirst();
      }

      @Override
      public void beforeFirst() {
        s.beforeFirst();
        moregroups = s.next();
      }

      /*
       * read until a new group value appears.
       * moregroups is always true until the underlying scan finishes scanning
       */
      @Override
      public boolean next() {
        if (!moregroups)
          return false;
        for (AggregationFn fn : aggfns)
          fn.processFirst(s);
        groupval = new GroupValue(s, groupfields);
        while (moregroups = s.next()) {
          GroupValue gv = new GroupValue(s, groupfields);
          if (!groupval.equals(gv))
            break;
          for (AggregationFn fn : aggfns)
            fn.processNext(s);
        }
        return true;
      }

      @Override
      public int getInt(String fldname) {
        return getVal(fldname).asInt();
      }

      @Override
      public String getString(String fldname) {
        return getVal(fldname).asString();
      }

      @Override
      public Constant getVal(String fldname) {
        if (groupfields.contains(fldname))
          return groupval.getVal(fldname);
        for (AggregationFn fn : aggfns)
          if (fn.fieldName().equals(fldname))
            return fn.value();
        throw new RuntimeException("field: " + fldname + " not found.");
      }

      @Override
      public boolean hasField(String fldname) {
        if (groupfields.contains(fldname))
          return true;
        for (AggregationFn fn : aggfns)
          if (fn.fieldName().equals(fldname))
            return true;
        return false;
      }

      @Override
      public void close() {
        s.close();
      }
    }
    ```
1. Add `materialize/GroupByPlan.java`

    ```java
    package simpledb.materialize;

    import java.util.List;

    import simpledb.plan.Plan;
    import simpledb.query.Scan;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    public class GroupByPlan implements Plan {
      private Plan p;
      private List<String> groupfields;
      private List<AggregationFn> aggfns;
      private Schema sch = new Schema(); // contains groupfields & aggregation fields

      public GroupByPlan(Transaction tx, Plan p, List<String> groupfields, List<AggregationFn> aggfns) {
        this.p = new SortPlan(tx, p, groupfields);
        this.groupfields = groupfields;
        this.aggfns = aggfns;
        for (String fldname : groupfields)
          sch.add(fldname, p.schema());
        for (AggregationFn fn : aggfns)
          sch.addIntField(fn.fieldName());
      }

      @Override
      public Scan open() {
        Scan s = p.open();
        return new GroupByScan(s, groupfields, aggfns);
      }

      @Override
      public int blockAccessed() {
        return p.blockAccessed();
      }

      @Override
      public int recordsOutput() {
        int numgroups = 1;
        for (String fldname : groupfields)
          numgroups += p.distinctValues(fldname);
        return numgroups;
      }

      @Override
      public int distinctValues(String fldname) {
        if (p.schema().hasField(fldname))
          return p.distinctValues(fldname);
        else
          return recordsOutput();
      }

      /*
       * Return the schema of the output table.
       * The schema consists of the group fields and
       * aggregation result fields.
       */
      @Override
      public Schema schema() {
        return sch;
      }
    }
    ```

1. Add `materialize/CountFn.java`

    ```java
    package simpledb.materialize;

    import simpledb.query.Constant;
    import simpledb.query.Scan;

    public class CountFn implements AggregationFn {
      private String fldname;
      private int count;

      public CountFn(String fldname) {
        this.fldname = fldname;
      }

      @Override
      public void processFirst(Scan s) {
        count = 1;
      }

      @Override
      public void processNext(Scan s) {
        count++;
      }

      @Override
      public String fieldName() {
        return "countof" + fldname;
      }

      @Override
      public Constant value() {
        return new Constant(count);
      }
    }
    ```

1. Add `materialize/MaxFn.java`

    ```java
    package simpledb.materialize;

    import simpledb.query.Constant;
    import simpledb.query.Scan;

    public class MaxFn implements AggregationFn {
      private String fldname;
      private Constant val;

      public MaxFn(String fldname) {
        this.fldname = fldname;
      }

      @Override
      public void processFirst(Scan s) {
        val = s.getVal(fldname);
      }

      @Override
      public void processNext(Scan s) {
        Constant newval = s.getVal(fldname);
        if (val.compareTo(newval) > 0)
          val = newval;
      }

      @Override
      public String fieldName() {
        return "maxof" + fldname;
      }

      @Override
      public Constant value() {
        return val;
      }
    }
    ```

1. Add tests

    ```java
    package simpledb.materialize;

    import static org.junit.jupiter.api.Assertions.assertEquals;
    import static org.junit.jupiter.api.Assertions.assertFalse;
    import static org.junit.jupiter.api.Assertions.assertTrue;
    import static org.mockito.Mockito.when;

    import java.util.Arrays;

    import org.junit.jupiter.api.Test;
    import org.junit.jupiter.api.extension.ExtendWith;
    import org.mockito.Mock;
    import org.mockito.junit.jupiter.MockitoExtension;

    import simpledb.query.Constant;
    import simpledb.query.Scan;

    @ExtendWith(MockitoExtension.class)
    public class GroupByScanTest {
      @Mock
      private Scan scan;

      @Mock
      private AggregationFn aggfn;

      @Test
      public void testGroupByScan() {
        when(scan.next()).thenReturn(true, true, true, true, false); // 4 records
        when(scan.getVal("gf")).thenReturn(new Constant(1),new Constant(1) , new Constant(2), new Constant(2));
        when(aggfn.fieldName()).thenReturn("countoffld");
        when(aggfn.value()).thenReturn(new Constant(10), new Constant(20));

        GroupByScan gbs = new GroupByScan(scan, Arrays.asList("gf"), Arrays.asList(aggfn));

        assertTrue(gbs.next()); // gf=1
        assertEquals(1, gbs.getInt("gf"));
        assertEquals(new Constant(10), gbs.getVal("countoffld"));
        assertTrue(gbs.next()); // gf=2
        assertEquals(2, gbs.getInt("gf"));
        assertEquals(new Constant(20), gbs.getVal("countoffld"));
        assertFalse(gbs.next());
      }
    }
    ```

    ```java
    package simpledb.materialize;

    import static org.junit.jupiter.api.Assertions.assertEquals;

    import org.junit.jupiter.api.Test;

    import simpledb.query.Constant;

    public class CountFnTest {

      @Test
      public void testCountFn() {
        AggregationFn fn = new CountFn("fld");
        assertEquals("countoffld", fn.fieldName());
        fn.processFirst(null);
        assertEquals(new Constant(1), fn.value());
        fn.processNext(null);
        assertEquals(new Constant(2), fn.value());
      }
    }
    ```

1. Run test
    ```
    rm -rf app/datadir && ./gradlew test
    ```

1. Add the following code to `App.java`

    ```java
    System.out.println("13.3. GroupBy and Aggregation --------------------");
    tx = new Transaction(fm, lm, bm);
    plan = new TablePlan(tx, "T3", metadataMgr);
    AggregationFn aggfn = new CountFn("fld2");
    plan = new GroupByPlan(tx, plan, Arrays.asList("fld1"), Arrays.asList(aggfn));
    scan = plan.open();
    while (scan.next())
      System.out
        .println("aggregation result: groupby: " + scan.getVal("fld1") + ", count: " + scan.getVal(countfn.fieldName()) + ", max: " + scan.getVal(maxfn.fieldName()));

    scan.close();
    tx.commit();
    ```

1. Run

    ```
    rm -rf app/datadir && ./gradlew run
    ```

    ```
    aggregation result: groupby: rec0, count: 1, max: 0
    aggregation result: groupby: rec1, count: 1, max: 1
    ```

## 13.4. MergeJoin

1. Add `savePosition` and `restorePosition` to `SortScan`

    ```java
    public void restorePosition() {
      RID rid1 = savedposition.get(0);
      RID rid2 = savedposition.get(1);
      s1.moveToRid(rid1);
      if (rid2 != null)
        s2.moveToRid(rid2);
    }

    public void savePosition() {
      RID rid1 = s1.getRid();
      RID rid2 = (s2 == null) ? null : s2.getRid();
      savedposition = Arrays.asList(rid1, rid2);
    }
    ```
1. Add `materialize/MergeJoinScan.java`

    ```java
    package simpledb.materialize;

    import simpledb.query.Constant;
    import simpledb.query.Scan;

    public class MergeJoinScan implements Scan {
      private Scan s1;
      private SortScan s2;
      private String fldname1;
      private String fldname2;
      private Constant joinval = null;

      public MergeJoinScan(Scan s1, SortScan s2, String fldname1, String fldname2) {
        this.s1 = s1;
        this.s2 = s2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        beforeFirst();
      }

      @Override
      public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
      }

      /*
       * 1. If the next RHS record has the same join value, then move it.
       * 2. If the next LHS record has the same join value, move the RHS scan
       * back to the first record having that join value.
       * 3. Otherwise, repeatedly move the scan having the smallest value until
       * a common join value is found.
       * 4. If there's no more records in both RHS and LHS, return false.
       */
      @Override
      public boolean next() {
        boolean hasmore2 = s2.next();
        if (hasmore2 && s2.getVal(fldname2).equals(joinval)) {
          System.out.println("[MergeJoinScan] next increments RHS joinval: " + joinval);
          return true;
        }

        boolean hasmore1 = s1.next();
        if (hasmore1 && s1.getVal(fldname1).equals(joinval)) {
          s2.restorePosition();
          System.out.println(
              "[MergeJoinScan] next increments LHS and move RHS back to the starting point of joinval: " + joinval);
          return true;
        }

        while (hasmore1 && hasmore2) {
          Constant v1 = s1.getVal(fldname1);
          Constant v2 = s2.getVal(fldname2);
          if (v1.compareTo(v2) < 0)
            hasmore1 = s1.next();
          else if (v1.compareTo(v2) > 0)
            hasmore2 = s2.next();
          else {
            s2.savePosition();
            joinval = s2.getVal(fldname2);
            System.out.println("[MergeJoinScan] next update joinval: " + joinval);
            return true;
          }
        }
        System.out.println("[MergeJoinScan] next no more next: " + joinval);
        return false;
      }

      @Override
      public int getInt(String fldname) {
        if (s1.hasField(fldname))
          return s1.getInt(fldname);
        else
          return s2.getInt(fldname);
      }

      @Override
      public String getString(String fldname) {
        if (s1.hasField(fldname))
          return s1.getString(fldname);
        else
          return s2.getString(fldname);
      }

      @Override
      public Constant getVal(String fldname) {
        if (s1.hasField(fldname))
          return s1.getVal(fldname);
        else
          return s2.getVal(fldname);
      }

      @Override
      public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
      }

      @Override
      public void close() {
        s1.close();
        s2.close();
      }
    }
    ```

1. Add `materialize/MergeJoinPlan.java`

    ```java
    package simpledb.materialize;

    import java.util.Arrays;
    import java.util.List;

    import simpledb.plan.Plan;
    import simpledb.query.Scan;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    public class MergeJoinPlan implements Plan {
      private Plan p1, p2;
      private String fldname1
      private String fldname2;
      private Schema sch = new Schema();

      public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this.fldname1 = fldname1;
        List<String> sortlist1 = Arrays.asList(fldname1);
        this.p1 = new SortPlan(tx, p1, sortlist1);

        this.fldname2 = fldname2;
        List<String> sortlist2 = Arrays.asList(fldname2);
        this.p2 = new SortPlan(tx, p2, sortlist2);

        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
      }

      @Override
      public Scan open() {
        Scan s1 = p1.open();
        SortScan s2 = (SortScan) p2.open();
        return new MergeJoinScan(s1, s2, fldname1, fldname2);
      }

      @Override
      public int blockAccessed() {
        return p1.blockAccessed() + p2.blockAccessed();
      }

      @Override
      public int recordsOutput() {
        int maxvals = Math.max(p1.distinctValues(fldname1),
            p2.distinctValues(fldname2));
        return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
      }

      @Override
      public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname))
          return p1.distinctValues(fldname);
        else
          return p2.distinctValues(fldname);
      }

      @Override
      public Schema schema() {
        return sch;
      }
    }
    ```
1. Add the following to `App.java`

    ```java
    System.out.println("13.4. MergeJoin --------------------");
    bm = new BufferMgr(fm, lm, 16); // buffer 8 is not enough
    tx = new Transaction(fm, lm, bm);
    p1 = new TablePlan(tx, "T1", metadataMgr); // T1 A:int, B:String
    p2 = new TablePlan(tx, "T2", metadataMgr); // T3 fld1:String, fld2:int
    plan = new MergeJoinPlan(tx, p1, p2, "A", "C"); // JOIN ON T1.A = T3.fld2
    scan = plan.open();
    scan.beforeFirst();
    while (scan.next()) {
      System.out.print("merged result:");
      for (String fldname : p1.schema().fields())
        System.out.print(" T1." + fldname + ": " + scan.getVal(fldname) + ",");
      for (String fldname : p2.schema().fields())
        System.out.print(" T2." + fldname + ": " + scan.getVal(fldname) + ",");
      System.out.println("");
    }
    scan.close();
    tx.commit();
    ```

    The codes merge `T1` and `T2` on `T1.A = T2.C`.

1. Run test

    ```
    rm -rf app/datadir && ./gradlew run
    ```

    The result may be like the following but it's not deterministic as the data in `T1` is random.
    ```
    [MergeJoinScan] next update joinval: 1
    merged result: T1.A: 1, T1.B: rec1, T2.C: 1, T2.D: rec1,
    [MergeJoinScan] next update joinval: 3
    merged result: T1.A: 3, T1.B: rec3, T2.C: 3, T2.D: rec3,
    [MergeJoinScan] next update joinval: 6
    merged result: T1.A: 6, T1.B: rec6, T2.C: 6, T2.D: rec6,
    [MergeJoinScan] next update joinval: 9
    merged result: T1.A: 9, T1.B: rec9, T2.C: 9, T2.D: rec9,
    [MergeJoinScan] next no more next: 9
    ```


## Exercise 13.8. Sort empty table

1. Add the following code to `App.java`

    ```java
    // Exercise 13.8. Sort empty table
    tx = new Transaction(fm, lm, bm);
    sch = new Schema();
    sch.addIntField("intfld");
    layout = new Layout(sch);
    metadataMgr.createTable("emptytable", sch, tx);
    plan = new TablePlan(tx, "emptytable", metadataMgr);
    plan = new SortPlan(tx, plan, Arrays.asList("intfld"));
    scan = plan.open();
    while (scan.next())
      System.out.println(scan.getInt("intfld"));
    scan.close();
    tx.commit();
    ```
1. Run
    ```
    Exception in thread "main" java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0
            at java.base/jdk.internal.util.Preconditions.outOfBounds(Preconditions.java:100)
            at java.base/jdk.internal.util.Preconditions.outOfBoundsCheckIndex(Preconditions.java:106)
            at java.base/jdk.internal.util.Preconditions.checkIndex(Preconditions.java:302)
            at java.base/java.util.Objects.checkIndex(Objects.java:385)
            at java.base/java.util.ArrayList.get(ArrayList.java:427)
            at simpledb.materialize.SortScan.<init>(SortScan.java:27)
            at simpledb.materialize.SortPlan.open(SortPlan.java:37)
            at simpledb.App.main(App.java:570)
    ```

1. Update `SortScan.java`

    ```java
    if (!runs.isEmpty()) {
      s1 = runs.get(0).open();
      hasmore1 = s1.next();
    }
    ```

    ```java
    if (s1 != null)
        s1.close()
    ```

1. Run `./gradlew run` -> no error

## Exercise 13.9. Add `preprocessingCost` to `Plan` interface

1. Add `preprocessingCost()` to Plan interface.
1. Add `preprocessingCost()` to Plan implementations.
    1. `SortPlan`
        ```java
        /*
         * Ref: 13.4.4. The Cost of Mergesort
         * Condition:
         * 1. The algorithm merges k runs at a time.
         * 2. There are R initial runs.
         * 3. The materialized input records require B block.
         * Split phase:
         * 1. B block accesses
         * 2. The cost of the input
         * Sort iteration: logkR iterations
         * 1. 2B block accesses for each iteration * (logkR - 1)
         */
        @Override
        public int preprocessingCost() {
          int k = 2; // merge 2 runs at once
          int r = blockAccessed() / 2; // estimated initial runs
          int splitCost = blockAccessed() + p.blockAccessed(); // cost of writing temptable + input cost
          double sortCost = 2 * blockAccessed() * (Math.log(r) / Math.log(k) - 1);
          return (int) (splitCost + sortCost);
        }
        ```
    1. `MaterializePlan`
        ```java
        public int preprocessingCost() {
          return srcplan.blockAccessed() + blockAccessed();
        }
        ```
    1. `MergeJoinPlan`
        ```java
        public int preprocessingCost() {
          return p1.preprocessingCost() + p2.preprocessingCost();
        }
        ```

## Exercise 13.10. Construct initial runs with one-block long (ToDo)

Ref **Fig. 13.6.** in *13.4.3. Improving the Mergesort Algorithem*

Repeat until there are no more input records:
1. Read a block's worth of input records into a new temporary table.
1. Sort those records using an in-memory sorting algorithm.
1. Save the one-block temporary table to disk.

- https://favtutor.com/blogs/sorting-algorithms-java
- https://en.wikipedia.org/wiki/Heapsort
