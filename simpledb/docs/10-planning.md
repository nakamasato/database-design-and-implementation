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
#### 10.1.3. ProjectPlan
#### 10.1.4. ProductPlan

### 10.2. Planner

#### 10.2.1 BasicQueryPlanner
#### 10.2.2. BasicUpdatePlanner
#### 10.2.3. QueryPlanner
#### 10.2.4. UpdatePlanner
