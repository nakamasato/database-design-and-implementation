## Chapter 10: Planning

### Overview

![](planner.drawio.svg)

### 10.1. Plan

#### 10.1.1. TablePlan

1. Add `Plan` interface `plan/Plan.java`

    ```java
    package simpledb.plan;

    import simpledb.query.Scan;
    import simpledb.record.Schema;

    /*
     * The interface implemented by each query plan.
     * There is a Plan class for each relational algebra operator.
     */
    public interface Plan {
      /*
       * Open a scan corresponding to this plan
       */
      public Scan open();

      /*
       * The estimated number of block accesses
       * that will occur when the scan is executed.
       * This value is used to calculate the estimated cost of the plan
       */
      public int blockAccessed();

      /*
       * The estimated number of output records.
       * This value is used to calculate the estimated cost of the plan
       */
      public int recordsOutput();

      /*
       * The estimated number of distinct records for the specified field
       * This value is used to calculate the estimated cost of the plan
       */
      public int distinctValues(String fldname);

      /*
       * Schema of output table
       */
      public Schema schema();
    }
    ```

1. Add `plan/TablePlan.java`

    ```java
    package simpledb.plan;

    import simpledb.metadata.MetadataMgr;
    import simpledb.metadata.StatInfo;
    import simpledb.query.Scan;
    import simpledb.record.Layout;
    import simpledb.record.Schema;
    import simpledb.record.TableScan;
    import simpledb.tx.Transaction;

    public class TablePlan implements Plan {
      private String tblname;
      private Transaction tx;
      private Layout layout;
      private StatInfo si;

      public TablePlan(Transaction tx, String tblname, MetadataMgr md) {
        this.tblname = tblname;
        this.tx = tx;
        layout = md.getLayout(tblname, tx);
        si = md.getStatInfo(tblname, layout, tx);
      }

      @Override
      public Scan open() {
        return new TableScan(tx, tblname, layout);
      }

      @Override
      public int blockAccessed() {
        return si.blocksAccessed();
      }

      @Override
      public int recordsOutput() {
        return si.recordsOutput();
      }

      @Override
      public int distinctValues(String fldname) {
        return si.distinctValues(fldname);
      }

      @Override
      public Schema schema() {
        return layout.schema();
      }
    }
    ```

1. Add the following code to `App.java`

    ```java
    // 10. Planning
    System.out.println("10.1. TablePlan-------------");
    metadataMgr.createTable("T1", sch1, tx); // create table in because tabcat doesn't have a record for T1 created above
    Plan p1 = new TablePlan(tx, "T1", metadataMgr);

    System.out.println("R(p1): " + p1.recordsOutput());
    System.out.println("B(p1): " + p1.blockAccessed());
    for (String fldname : sch1.fields())
      System.out.println("V(p1, " + fldname + "): " + p1.distinctValues(fldname));

    tx.commit();
    ```

1. Run
    ```
    ./gradlew run
    ```

    You can see output records, block accessed, and the distinct value for each fields:
    ```
    R(p1): 10
    B(p1): 1
    V(p1, A): 4
    V(p1, B): 4
    ```

#### 10.1.2. SelectPlan

1. Add the reductionFactor to `query/Term.java`

    ```java
    /*
     * Calculate the extent to which selecting on the term reduces the number of
     * records output by a query.
     * If reduction factor is 2, the term cuts the size in half.
     */
    public int reductionFactor(Plan p) {
      String lhsName;
      String rhsName;
      // max of 1/(distinct values of the field)
      if (lhs.isFieldName() && rhs.isFieldName()) {
        lhsName = lhs.asFieldName();
        rhsName = rhs.asFieldName();
        return Math.max(p.distinctValues(lhsName), p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
        lhsName = lhs.asFieldName();
        return p.distinctValues(lhsName); // 1/(distinct values of the field)
      }
      if (rhs.isFieldName()) {
        rhsName = rhs.asFieldName();
        return p.distinctValues(rhsName); // 1/(distinct values of the field)
      }
      if (lhs.asConstant().equals(rhs.asConstant())) // no change
        return 1;
      else
        return Integer.MAX_VALUE; // not match -> infinite reduction
    }

    /*
     * If the term is in the form of F=c, return c
     * otherwise, return null.
     */
    public Constant equatesWithConstant(String fldname) {
      if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && !rhs.isFieldName())
        return rhs.asConstant();
      else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && !lhs.isFieldName())
        return lhs.asConstant();
      else
        return null;
    }

    /*
     * If the term is in the form of F1=F2, return the field name
     * otherwise, return null
     */
    public String equatesWithField(String fldname) {
      if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && rhs.isFieldName())
        return rhs.asFieldName();
      else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && lhs.isFieldName())
        return lhs.asFieldName();
      else
        return null;
    }
    ```

1.  Add the reductionFactor to `query/Predicate.java`

    ```java
    /*
     * The product of all the term's reduction factors
     */
    public int reductionFactor(Plan p) {
      int factor = 1;
      for (Term t : terms)
        factor *= t.reductionFactor(p);
      return factor;
    }

    /*
     * Determine if there is a term of the form "F=c"
     * where F is the specified field and c is some constant.
     * If true, return the constant, otherwise return null.
     */
    public Object equatesWithConstant(String fldname) {
      for (Term t : terms) {
        Constant c = t.equatesWithConstant(fldname);
        if (c != null)
          return c;
      }
      return null;
    }

    /*
     * Determine if there is a term of the form "F1=F2"
     * where F1 is the specified field and F2 is another.
     * If true, return the F2 field name, otherwise return null.
     */
    public String equatesWithField(String fldname) {
      for (Term t : terms) {
        String s = t.equatesWithField(fldname);
        if (s != null)
          return s;
      }
      return null;
    }
    ```

1. Add `plan/SelectPlan.java`

    ```java
    package simpledb.plan;

    import simpledb.query.Predicate;
    import simpledb.query.Scan;
    import simpledb.query.SelectScan;
    import simpledb.record.Schema;

    /*
     * The Plan class corresponding to the select
     * relational algebra operator
     */
    public class SelectPlan implements Plan {
      private Plan p;
      private Predicate pred;

      public SelectPlan(Plan p, Predicate pred) {
        this.p = p;
        this.pred = pred;
      }

      @Override
      public Scan open() {
        Scan s = p.open();
        return new SelectScan(s, pred);
      }

      @Override
      public int blockAccessed() {
        return p.blockAccessed();
      }

      /*
       * Estimate the number of output records in the selectiion,
       * which is determined by the reduction factor of the predicate.
       */
      @Override
      public int recordsOutput() {
        return p.recordsOutput() / pred.reductionFactor(p);
      }

      @Override
      public int distinctValues(String fldname) {
        if (pred.equatesWithConstant(fldname) != null)
          return 1;
        else {
          String fldname2 = pred.equatesWithField(fldname);
          if (fldname2 != null)
            return Math.min(p.distinctValues(fldname), p.distinctValues(fldname2));
          else
            return p.distinctValues(fldname);
        }
      }

      @Override
      public Schema schema() {
        return p.schema();
      }
    }
    ```

1. Add the following code to `App.java` (before the last `tx.commit()`)

    ```java
    // Select node
    System.out.println("10.1.2. SelectPlan-------------");
    t = new Term(new Expression("A"), new Expression(new Constant(5)));
    pred = new Predicate(t);
    Plan p2 = new SelectPlan(p1, pred);
    System.out.println("R(p2): " + p2.recordsOutput());
    System.out.println("B(p2): " + p2.blockAccessed());
    for (String fldname : p2.schema().fields())
      System.out.println("V(p2, " + fldname + "): " + p2.distinctValues(fldname));
    ```

1. Run.

    ```
    ./gradlew run
    ```

    ```
    10.1.2. SelectPlan-------------
    R(p2): 2
    B(p2): 1
    V(p2, A): 1
    V(p2, B): 4
    ```

#### 10.1.3. ProjectPlan

1. Add `ProjectPlan`

    ```java
    package simpledb.plan;

    import java.util.List;

    import simpledb.query.ProjectScan;
    import simpledb.query.Scan;
    import simpledb.record.Schema;

    /*
     * Plan class corresponding to the project
     * relational algebra operator
     */
    public class ProjectPlan implements Plan {
      private Plan p;
      private Schema schema = new Schema();

      public ProjectPlan(Plan p, List<String> fieldlist) {
        this.p = p;
        for (String fldname : fieldlist)
          schema.add(fldname, p.schema());
      }

      @Override
      public Scan open() {
        Scan s = p.open();
        return new ProjectScan(s, schema.fields());
      }

      @Override
      public int blockAccessed() {
        return p.blockAccessed();
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
        return schema;
      }
    }
    ```

1. Add the following to `App.java` (before the last `tx.commit()`)

    ```java
    // Project node
    System.out.println("10.1.3. ProjectPlan-------------");
    ProjectPlan p3 = new ProjectPlan(p2, fields);
    System.out.println("R(p3): " + p3.recordsOutput());
    System.out.println("B(p3): " + p3.blockAccessed());
    for (String fldname : p3.schema().fields())
      System.out.println("V(p2, " + fldname + "): " + p3.distinctValues(fldname));

    Scan s = p3.open();
    while (s.next())
      System.out.println(s.getString("B"));
    s.close();
    ```

1. Run
    ```
    ./gradlew run
    ```

    ```
    10.1.3. ProjectPlan-------------
    R(p3): 2
    B(p3): 1
    V(p2, B): 4
    ...
    rec5
    rec5
    ```
#### 10.1.4. ProductPlan

1. Add `plan/ProductPlan.java`

    ```java
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
    }
    ```
1. Add the following code to `App.java`

    ```java
    // Product node
    System.out.println("10.1.4. ProductPlan-------------");
    metadataMgr.createTable("T2", sch2, tx); // tabcat doesn't have a record for T2 created above
    Plan p4 = new TablePlan(tx, "T2", metadataMgr);
    Plan p5 = new ProductPlan(p1, p4);
    Plan p6 = new SelectPlan(p5, pred);
    System.out.println("R(p6): " + p6.recordsOutput());
    System.out.println("B(p6): " + p6.blockAccessed());
    for (String fldname : p6.schema().fields())
      System.out.println("V(p6, " + fldname + "): " + p6.distinctValues(fldname));

    s = p6.open();
    s.beforeFirst(); // this is necessary for p1 to move to the first position
    while (s.next())
      System.out.println(
          "A: " + s.getInt("A") + ", B: " + s.getString("B") + ", C: " + s.getInt("C") + ", D: " + s.getString("D"));
    s.close();
    ```
1. Run

    ```
    ./gradlew run
    ```

    ```
    R(p6): 25
    B(p6): 11
    V(p6, A): 1
    V(p6, B): 4
    V(p6, C): 4
    V(p6, D): 4
    ```
### 10.2. Planner

#### 10.2.1 BasicQueryPlanner

1. Add `QueryPlanner` interface. (`plan/QueryPlanner.java`)

    ```java
    package simpledb.plan;

    import simpledb.parse.QueryData;
    import simpledb.tx.Transaction;

    /*
     * Interface implemented by planners for
     * the SQL select statement.
     */
    public interface QueryPlanner {
      public Plan createPlan(QueryData data, Transaction tx);
    }
    ```

1. Add `plan/BasicQueryPlanner.java`

    ```java
    package simpledb.plan;

    import java.util.ArrayList;
    import java.util.List;

    import simpledb.metadata.MetadataMgr;
    import simpledb.parse.Parser;
    import simpledb.parse.QueryData;
    import simpledb.tx.Transaction;

    /*
     * Simplest and most naive query planner
     */
    public class BasicQueryPlanner implements QueryPlanner {
      private MetadataMgr mdm;

      public BasicQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
      }

      @Override
      public Plan createPlan(QueryData data, Transaction tx) {
        // Step 1: Create a plan for each mentioned table or view.
        List<Plan> plans = new ArrayList<>();
        for (String tblname : data.tables()) {
          String viewdef = mdm.getViewDef(tblname, tx);
          if (viewdef != null) {
            Parser parser = new Parser(viewdef);
            QueryData viewData = parser.query();
            plans.add(createPlan(viewData, tx));
          } else
            plans.add(new TablePlan(tx, tblname, mdm));
        }

        // Step 2: Create product of all table plans
        // ProductPlan(...ProductPlan(ProductPlan(p0, p1), p2, p3,...)
        // The order is arbitrary as tables() returns Collection<String>
        Plan p = plans.remove(0);
        for (Plan nextplan : plans)
          p = new ProductPlan(p, nextplan);

        // Step 3: Add a select plan for the predicate
        p = new SelectPlan(p, data.predicate());

        // Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
      }
    }
    ```

1. Add test `plan/BasicQueryPlannertest.java`
    ```java
    package simpledb.plan;

    import static org.junit.jupiter.api.Assertions.assertEquals;
    import static org.junit.jupiter.api.Assertions.assertTrue;
    import static org.mockito.Mockito.when;

    import java.util.Arrays;

    import org.junit.jupiter.api.Test;
    import org.junit.jupiter.api.extension.ExtendWith;
    import org.mockito.Mock;
    import org.mockito.junit.jupiter.MockitoExtension;

    import simpledb.metadata.MetadataMgr;
    import simpledb.metadata.StatInfo;
    import simpledb.parse.QueryData;
    import simpledb.query.Constant;
    import simpledb.query.Expression;
    import simpledb.query.Predicate;
    import simpledb.query.Term;
    import simpledb.record.Layout;
    import simpledb.record.Schema;

    @ExtendWith(MockitoExtension.class)
    public class BasicQueryPlannerTest {

      @Mock
      private MetadataMgr mdm;

      /*
       * Test case of SQL: select fld1 from tbl1 where fld1 = 1
       * ProjectPlan(SelectPlan(TablePlan(tx, "tbl1", mdm), pred), fields)
       */
      @Test
      public void testSingleTable() {
        Schema sch = new Schema();
        sch.addIntField("fld1");
        Layout layout = new Layout(sch);
        StatInfo si = new StatInfo(10, 100);
        when(mdm.getViewDef("tbl1", null)).thenReturn(null);
        when(mdm.getLayout("tbl1", null)).thenReturn(layout);
        when(mdm.getStatInfo("tbl1", layout, null)).thenReturn(si);

        BasicQueryPlanner basicQueryPlanner = new BasicQueryPlanner(mdm);
        Term term = new Term(new Expression("fld1"), new Expression(new Constant(1)));
        Predicate pred = new Predicate(term);
        QueryData qd = new QueryData(Arrays.asList("fld1"), Arrays.asList("tbl1"), pred);
        Plan plan = basicQueryPlanner.createPlan(qd, null);

        assertTrue(plan instanceof ProjectPlan);
        assertEquals(1, plan.schema().fields().size()); // fld1
        assertEquals(10, plan.blockAccessed()); // from StatInfo
        assertEquals(2, plan.recordsOutput());
        // TablePlan.recordsOutput / pred.reductionFactor(tableplan) = numRecs /
        // term.reductionFactor = tableplan.distinctValues(fld1) =
        // statinfo.distinctvalues(fld1)
        // 100 / (1 + (100 / 3)) = 2.91 -> 2
        assertEquals(1, plan.distinctValues("fld1")); // the result only contains fld1=1
      }

      /*
       * Test case of SQL: select fld1 from tbl1, tbl2 where fld1 = fld2
       * ProjectPlan(SelectPlan(Product(TablePlan(tx, "tbl1", mdm), TablePlan(tx,
       * "tbl2", mdm)), pred), fields)
       */
      @Test
      public void testMultipleTables() {
        Schema sch1 = new Schema();
        sch1.addIntField("fld1");
        Layout layout1 = new Layout(sch1);
        Schema sch2 = new Schema();
        sch1.addIntField("fld2");
        Layout layout2 = new Layout(sch2);
        StatInfo si1 = new StatInfo(10, 100);
        StatInfo si2 = new StatInfo(30, 900);
        when(mdm.getViewDef("tbl1", null)).thenReturn(null);
        when(mdm.getLayout("tbl1", null)).thenReturn(layout1);
        when(mdm.getStatInfo("tbl1", layout1, null)).thenReturn(si1);
        when(mdm.getViewDef("tbl2", null)).thenReturn(null);
        when(mdm.getLayout("tbl2", null)).thenReturn(layout2);
        when(mdm.getStatInfo("tbl2", layout2, null)).thenReturn(si2);

        BasicQueryPlanner basicQueryPlanner = new BasicQueryPlanner(mdm);
        Term term = new Term(new Expression("fld1"), new Expression("fld2"));
        Predicate pred = new Predicate(term);
        QueryData qd = new QueryData(Arrays.asList("fld1", "fld2"), Arrays.asList("tbl1", "tbl2"), pred);
        Plan plan = basicQueryPlanner.createPlan(qd, null);

        assertTrue(plan instanceof ProjectPlan);
        assertEquals(2, plan.schema().fields().size()); // fld1, fld2
        assertEquals(3010, plan.blockAccessed()); // ProductPlan.blockaccessed() = B(t1) + R(t1) * B(t2) = 10 + 100 * 30

        assertEquals(2647, plan.recordsOutput());
        // R(ProductPlan) / pred.reductionFactor(productplan)
        // P(ProductPlan): R(t1) * R(t2) = 90000
        // pred.reductionFactor(productplan): Min(V(t1, "fld1"), V(t2, "fld2")) =
        // Min(34.3, 301) = 34
        // 90000 / 34 = 2647
        // int expectedRecordOutput = si1.recordsOutput() * si2.recordsOutput() /
        // Math.min(si1.distinctValues("fld1"), si2.distinctValues("fld2")));

        int minDistinctVal = Math.min(si2.distinctValues("fld2"), si1.distinctValues("fld1"));
        assertEquals(minDistinctVal, plan.distinctValues("fld1"));
        assertEquals(minDistinctVal, plan.distinctValues("fld2"));
      }
    }
    ```

#### 10.2.2. BasicUpdatePlanner

1. Add `UpdatePlanner` interface `plan/UpdatePlanner.java`

    ```java
    package simpledb.plan;

    import simpledb.parse.CreateIndexData;
    import simpledb.parse.CreateTableData;
    import simpledb.parse.CreateViewData;
    import simpledb.parse.DeleteData;
    import simpledb.parse.InsertData;
    import simpledb.parse.ModifyData;
    import simpledb.tx.Transaction;

    /*
     * Interface implemented by planners for SQL
     * insert, delete, and modify statement
     */
    public interface UpdatePlanner {
      public int executeInsert(InsertData data, Transaction tx);

      public int executeDelete(DeleteData data, Transaction tx);

      public int executeModify(ModifyData data, Transaction tx);

      public int executeCreateTable(CreateTableData data, Transaction tx);

      public int executeCreateView(CreateViewData data, Transaction tx);

      public int executeCreateIndex(CreateIndexData data, Transaction tx);
    }
    ```

1. Add `plan/BasicUpdatePlanner.java`

    ```java
    package simpledb.plan;

    import java.util.Iterator;

    import simpledb.metadata.MetadataMgr;
    import simpledb.parse.CreateIndexData;
    import simpledb.parse.CreateTableData;
    import simpledb.parse.CreateViewData;
    import simpledb.parse.DeleteData;
    import simpledb.parse.InsertData;
    import simpledb.parse.ModifyData;
    import simpledb.query.Constant;
    import simpledb.query.UpdateScan;
    import simpledb.tx.Transaction;

    public class BasicUpdatePlanner implements UpdatePlanner {
      private MetadataMgr mdm;

      public BasicUpdatePlanner(MetadataMgr mdm) {
        this.mdm = mdm;
      }

      @Override
      public int executeDelete(DeleteData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), mdm);
        p = new SelectPlan(p, data.pred());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while (us.next()) {
          us.delete();
          count++;
        }
        us.close();
        return count;
      }

      @Override
      public int executeModify(ModifyData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), mdm);
        p = new SelectPlan(p, data.pred());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while (us.next()) {
          Constant val = data.newValue().evaluate(us);
          us.setVal(data.targetField(), val);
          count++;
        }
        us.close();
        return count;
      }

      @Override
      public int executeInsert(InsertData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), mdm);
        UpdateScan us = (UpdateScan) p.open();
        us.insert();
        Iterator<Constant> iter = data.vals().iterator();
        for (String fldname : data.fields()) {
          Constant val = iter.next();
          us.setVal(fldname, val);
        }
        us.close();
        return 1;
      }

      @Override
      public int executeCreateTable(CreateTableData data, Transaction tx) {
        mdm.createTable(data.tableName(), data.newSchema(), tx);
        return 0;
      }

      @Override
      public int executeCreateView(CreateViewData data, Transaction tx) {
        mdm.createView(data.viewName(), data.viewDef(), tx);
        return 0;
      }

      @Override
      public int executeCreateIndex(CreateIndexData data, Transaction tx) {
        mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
        return 0;
      }

    }
    ```

1. Add tests `plan/BasicUpdatePlannerTest.java`

    ```java
    package simpledb.plan;

    import static org.junit.jupiter.api.Assertions.assertEquals;
    import static org.mockito.Mockito.verify;
    import static org.mockito.Mockito.when;

    import java.util.Arrays;

    import org.junit.jupiter.api.Test;
    import org.junit.jupiter.api.extension.ExtendWith;
    import org.mockito.Mock;
    import org.mockito.junit.jupiter.MockitoExtension;

    import simpledb.file.BlockId;
    import simpledb.metadata.MetadataMgr;
    import simpledb.metadata.StatInfo;
    import simpledb.parse.CreateIndexData;
    import simpledb.parse.CreateTableData;
    import simpledb.parse.CreateViewData;
    import simpledb.parse.DeleteData;
    import simpledb.parse.InsertData;
    import simpledb.parse.ModifyData;
    import simpledb.parse.QueryData;
    import simpledb.query.Constant;
    import simpledb.query.Expression;
    import simpledb.query.Predicate;
    import simpledb.query.Term;
    import simpledb.record.Layout;
    import simpledb.record.RecordPage;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    @ExtendWith(MockitoExtension.class)
    public class BasicUpdatePlannerTest {
      @Mock
      private MetadataMgr mdm;

      @Mock
      private Transaction tx;

      /*
       * SQL: delete from tbl1 where fld1 = 1;
       * Record: 1, 1, 3, 3, 1, 3
       */
      @Test
      public void testDeleteData() {
        int numBlocks = 2;
        int numRecs = 6;
        int blockSize = 32; // 4 slots per block
        Schema sch = new Schema();
        sch.addIntField("fld1");
        Layout layout = new Layout(sch);
        int slotsize = layout.slotSize(); // slotsize = 8 (4 for flag, 4 for an int field)
        assertEquals(8, slotsize);
        StatInfo si = new StatInfo(numBlocks, numRecs); // 4, 2 slots in each block

        when(mdm.getLayout("tbl1", tx)).thenReturn(layout);
        when(mdm.getStatInfo("tbl1", layout, tx)).thenReturn(si);
        when(tx.size("tbl1.tbl")).thenReturn(numBlocks); // 2 blocks in tbl1.tbl
        when(tx.blockSize()).thenReturn(blockSize);

        // flag and value for each record
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 0
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 4)).thenReturn(1); // value for record 0
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 8)).thenReturn(RecordPage.USED); // flag for record 1
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 12)).thenReturn(1); // value for record 1
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 16)).thenReturn(RecordPage.USED); // flag for record 2
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 20)).thenReturn(3); // value for record 2
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 24)).thenReturn(RecordPage.USED); // flag for record 3
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 28)).thenReturn(3); // value for record 3
        when(tx.getInt(new BlockId("tbl1.tbl", 1), 0)).thenReturn(RecordPage.USED); // flag for record 4
        when(tx.getInt(new BlockId("tbl1.tbl", 1), 4)).thenReturn(1); // value for record 4
        when(tx.getInt(new BlockId("tbl1.tbl", 1), 8)).thenReturn(RecordPage.USED); // flag for record 5
        when(tx.getInt(new BlockId("tbl1.tbl", 1), 12)).thenReturn(3); // value for record 5

        BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
        Term term = new Term(new Expression("fld1"), new Expression(new Constant(1))); // fld1 = 1
        Predicate pred = new Predicate(term);
        DeleteData data = new DeleteData("tbl1", pred);
        int count = basicUpdatePlanner.executeDelete(data, tx);
        assertEquals(3, count);

        // Flag of record 0~2 are set to empty
        verify(tx).setInt(new BlockId("tbl1.tbl", 0), 0, RecordPage.EMPTY, true);
        verify(tx).setInt(new BlockId("tbl1.tbl", 0), 8, RecordPage.EMPTY, true);
        verify(tx).setInt(new BlockId("tbl1.tbl", 1), 0, RecordPage.EMPTY, true);
      }

      /*
       * SQL: update tbl1 set fld1 = 1000 where fld1 = 1;
       * Record: 1, 1, 3, 3, 1, 3
       */
      @Test
      public void testModifyData() {
        int numBlocks = 2;
        int numRecs = 6;
        int blockSize = 32; // 4 slots per block
        int newVal = 1000;
        Schema sch = new Schema();
        sch.addIntField("fld1");
        Layout layout = new Layout(sch);
        int slotsize = layout.slotSize(); // slotsize = 8 (4 for flag, 4 for an int field)
        assertEquals(8, slotsize);

        StatInfo si = new StatInfo(numBlocks, numRecs); // 4, 2 slots in each block
        when(mdm.getLayout("tbl1", tx)).thenReturn(layout);
        when(mdm.getStatInfo("tbl1", layout, tx)).thenReturn(si);
        when(tx.size("tbl1.tbl")).thenReturn(numBlocks); // 2 blocks in tbl1.tbl
        when(tx.blockSize()).thenReturn(blockSize);

        // flag and value for each record
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 0)).thenReturn(RecordPage.USED); // flag for record 0
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 4)).thenReturn(1); // value for record 0
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 8)).thenReturn(RecordPage.USED); // flag for record 1
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 12)).thenReturn(1); // value for record 1
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 16)).thenReturn(RecordPage.USED); // flag for record 2
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 20)).thenReturn(3); // value for record 2
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 24)).thenReturn(RecordPage.USED); // flag for record 3
        when(tx.getInt(new BlockId("tbl1.tbl", 0), 28)).thenReturn(3); // value for record 3
        when(tx.getInt(new BlockId("tbl1.tbl", 1), 0)).thenReturn(RecordPage.USED); // flag for record 4
        when(tx.getInt(new BlockId("tbl1.tbl", 1), 4)).thenReturn(1); // value for record 4
        when(tx.getInt(new BlockId("tbl1.tbl", 1), 8)).thenReturn(RecordPage.USED); // flag for record 5
        when(tx.getInt(new BlockId("tbl1.tbl", 1), 12)).thenReturn(3); // value for record 5

        BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
        Term term = new Term(new Expression("fld1"), new Expression(new Constant(1))); // fld1 = 1
        Predicate pred = new Predicate(term);
        ModifyData data = new ModifyData("tbl1", "fld1", new Expression(new Constant(newVal)), pred);
        int count = basicUpdatePlanner.executeModify(data, tx);
        assertEquals(3, count);

        // record 0~2 are updated with newVal
        verify(tx).setInt(new BlockId("tbl1.tbl", 0), 4, newVal, true);
        verify(tx).setInt(new BlockId("tbl1.tbl", 0), 12, newVal, true);
        verify(tx).setInt(new BlockId("tbl1.tbl", 1), 4, newVal, true);
      }

      /*
       * SQL: insert into tbl1 values (1, 'test');
       */
      @Test
      public void testInsertData() {
        int blockSize = 32;
        Schema sch = new Schema();
        sch.addIntField("fld1");
        sch.addStringField("fld2", 10);
        Layout layout = new Layout(sch);
        int slotsize = layout.slotSize(); // slotsize = 22 (4 for flag, 4 for an int field, 4 + 10 for a string field)
        assertEquals(22, slotsize);

        StatInfo si = new StatInfo(0, 0); // 4, 2 slots in each block
        when(mdm.getLayout("tbl1", tx)).thenReturn(layout);
        when(mdm.getStatInfo("tbl1", layout, tx)).thenReturn(si);
        when(tx.size("tbl1.tbl")).thenReturn(1); // 1 block in tbl1.tbl
        when(tx.blockSize()).thenReturn(blockSize);

        BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
        InsertData data = new InsertData("tbl1", Arrays.asList("fld1", "fld2"),
            Arrays.asList(new Constant(1), new Constant("test")));
        basicUpdatePlanner.executeInsert(data, tx);

        // record (1, 'test') is inserted
        verify(tx).setInt(new BlockId("tbl1.tbl", 0), 0, RecordPage.USED, true);
        verify(tx).setInt(new BlockId("tbl1.tbl", 0), 4, 1, true);
        verify(tx).setString(new BlockId("tbl1.tbl", 0), 8, "test", true);
      }

      /*
       * SQL: create table tbl1 (fld1 int, fld2 varchar(10))
       */
      @Test
      public void testCreateTable() {
        Schema sch = new Schema();
        sch.addIntField("fld1");
        sch.addStringField("fld2", 10);

        BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
        CreateTableData data = new CreateTableData("tbl1", sch);
        basicUpdatePlanner.executeCreateTable(data, tx);

        verify(mdm).createTable("tbl1", sch, tx);
      }

      /*
       * SQL: create view viewname as select fld1 from tbl1 where fld1 = 1;
       */
      @Test
      public void testCreateView() {
        BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
        Term term = new Term(new Expression("fld1"), new Expression(new Constant(1)));
        Predicate pred = new Predicate(term);
        QueryData queryData = new QueryData(Arrays.asList("fld1"), Arrays.asList("tbl1"), pred);
        CreateViewData data = new CreateViewData("viewname", queryData);
        basicUpdatePlanner.executeCreateView(data, tx);

        verify(mdm).createView("viewname", data.viewDef(), tx);
      }

      /*
       * SQL: create index test_idx on tbl1(fld1)
       */
      @Test
      public void testCreateIndex() {
        BasicUpdatePlanner basicUpdatePlanner = new BasicUpdatePlanner(mdm);
        CreateIndexData data = new CreateIndexData("test_idx", "tbl1", "fld1");
        basicUpdatePlanner.executeCreateIndex(data, tx);

        verify(mdm).createIndex("test_idx", "tbl1", "fld1", tx);
      }
    }
    ```

#### 10.2.3. Planner

1. add `plan/Planner.java`

    ```java
    package simpledb.plan;

    import simpledb.parse.CreateIndexData;
    import simpledb.parse.CreateTableData;
    import simpledb.parse.CreateViewData;
    import simpledb.parse.DeleteData;
    import simpledb.parse.InsertData;
    import simpledb.parse.ModifyData;
    import simpledb.parse.Parser;
    import simpledb.parse.QueryData;
    import simpledb.tx.Transaction;

    public class Planner {
      private QueryPlanner qplanner;
      private UpdatePlanner uplanner;

      public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
        this.qplanner = qplanner;
        this.uplanner = uplanner;
      }

      public Plan createQueryPlan(String qry, Transaction tx) {
        Parser parser = new Parser(qry);
        QueryData data = parser.query();
        verifyQuery(data);
        return qplanner.createPlan(data, tx);
      }

      public int executeUpdate(String cmd, Transaction tx) {
        Parser parser = new Parser(cmd);
        Object data = parser.updateCmd();
        verifyUpdate(data);
        if (data instanceof InsertData)
          return uplanner.executeInsert((InsertData) data, tx);
        else if (data instanceof DeleteData)
          return uplanner.executeDelete((DeleteData) data, tx);
        else if (data instanceof ModifyData)
          return uplanner.executeModify((ModifyData) data, tx);
        else if (data instanceof CreateTableData)
          return uplanner.executeCreateTable((CreateTableData) data, tx);
        else if (data instanceof CreateViewData)
          return uplanner.executeCreateView((CreateViewData) data, tx);
        else if (data instanceof CreateIndexData)
          return uplanner.executeCreateIndex((CreateIndexData) data, tx);
        else
          return 0;
      }

      private void verifyQuery(QueryData data) {
        // TODO
      }

      private void verifyUpdate(Object data) {
        // TODO
      }
    }
    ```

1. Add `PlannerTest.java`

    ```java
    package simpledb.plan;
    
    import static org.mockito.ArgumentMatchers.any;
    import static org.mockito.Mockito.verify;
    
    import org.junit.jupiter.api.Test;
    import org.junit.jupiter.api.extension.ExtendWith;
    import org.mockito.Mock;
    import org.mockito.junit.jupiter.MockitoExtension;
    
    import simpledb.parse.CreateIndexData;
    import simpledb.parse.CreateTableData;
    import simpledb.parse.CreateViewData;
    import simpledb.parse.DeleteData;
    import simpledb.parse.InsertData;
    import simpledb.parse.ModifyData;
    import simpledb.parse.QueryData;
    import simpledb.tx.Transaction;
    
    @ExtendWith(MockitoExtension.class)
    public class PlannerTest {
      @Mock
      QueryPlanner qplanner;
      @Mock
      UpdatePlanner uplanner;
      @Mock
      Transaction tx;
    
      @Test
      public void testCreateQueryPlan() {
        Planner planner = new Planner(qplanner, uplanner);
        planner.createQueryPlan("select fld1 from tbl1", tx);
        verify(qplanner).createPlan(any(QueryData.class), any(Transaction.class));
      }
    
      @Test
      public void testExecuteInsert() {
        Planner planner = new Planner(qplanner, uplanner);
        planner.executeUpdate("insert into tbl1(fld1) values (1)", tx);
        verify(uplanner).executeInsert(any(InsertData.class), any(Transaction.class));
      }
    
      @Test
      public void testExecuteDelete() {
        Planner planner = new Planner(qplanner, uplanner);
        planner.executeUpdate("delete from tbl1 where fld1 = 1", tx);
        verify(uplanner).executeDelete(any(DeleteData.class), any(Transaction.class));
      }
    
      @Test
      public void testExecuteModify() {
        Planner planner = new Planner(qplanner, uplanner);
        planner.executeUpdate("update tbl1 set fld1 = 1", tx);
        verify(uplanner).executeModify(any(ModifyData.class), any(Transaction.class));
      }
    
      @Test
      public void testExecuteCreateTable() {
        Planner planner = new Planner(qplanner, uplanner);
        planner.executeUpdate("create table tbl1 (fld1 int, fld2 varchar(10))", tx);
        verify(uplanner).executeCreateTable(any(CreateTableData.class), any(Transaction.class));
      }
    
      @Test
      public void testExecuteCreateView() {
        Planner planner = new Planner(qplanner, uplanner);
        planner.executeUpdate("create view testview as select fld1 from tbl1", tx);
        verify(uplanner).executeCreateView(any(CreateViewData.class), any(Transaction.class));
      }
    
      @Test
      public void testExecuteCreateIndex() {
        Planner planner = new Planner(qplanner, uplanner);
        planner.executeUpdate("create index test_idx on tbl1(fld1)", tx);
        verify(uplanner).executeCreateIndex(any(CreateIndexData.class), any(Transaction.class));
      }
    }
    ```
