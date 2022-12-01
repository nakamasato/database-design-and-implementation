## Chapter 8: Query Processing

### Overview
1. `Scan` interface (already implemented in [5.2. TableScan](05-transaction-management.md))
1. `UpdateScan` interface extends `Scan` (already implemented in [5.2. TableScan](05-transaction-management.md))
1. `TableScan` (already implemented in [5.2. TableScan](05-transaction-management.md))
1. `SelectScan`: `select * from <table_name> where <predicate>`
    ```
    Q3 = select(select(STUDENT, GradYear=2019), MajorId=10 or MajorId=20))
    ```
1. `ProjectScan`: `select <set of fields> from <table_name>`
    ```
    Q6 = project(select(STUDENT, MajorId=10), {SName})
    ```
1. `ProductScan`: `select * from <table_name1>, <table_name2>`
    ```
    Q8 = product(STUDENT, DEPT)
    ```
### 8.1. SelectScan
1. Create `query/Expression.java`
    1. Expression contains a pair of field name and value.
    ```java
    package simpledb.query;

    import simpledb.record.Schema;

    /*
     * SQL Expression
     */
    public class Expression {
      private Constant val = null;
      private String fldname = null;

      public Expression(Constant val) {
        this.val = val;
      }

      public Expression(String fldname) {
        this.fldname = fldname;
      }

      /*
       * Get the field value via Scan
       */
      public Constant evaluate(Scan s) {
        return (val != null) ? val : s.getVal(fldname);
      }

      public boolean isFieldName() {
        return fldname != null;
      }

      public Constant asConstant() {
        return val;
      }

      public String asFieldName() {
        return fldname;
      }

      /*
       * Check if the schema has the field
       */
      public boolean appliesTo(Schema sch) {
        return (val != null) || sch.hasField(fldname);
      }

      public String toString() {
        return (val != null) ? val.toString() : fldname;
      }

    }
    ```
1. Create `query/Term.java`
    1. Term is a comparison of two expressions. Only equality is supported.

    ```java
    package simpledb.query;

    import simpledb.record.Schema;

    /*
     * A term is a comparison between two expressions.
     * Now only supports equality
     */
    public class Term {
      private Expression lhs;
      private Expression rhs;

      public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
      }

      /*
       * Return true if the evaluation of the expressions
       * are equal.
       * This function is used to determined the result of Predicate.
       */
      public boolean isSatisfied(Scan s) {
        Constant lhsval = lhs.evaluate(s);
        Constant rhsval = rhs.evaluate(s);
        return rhsval.equals(lhsval);
      }

      /*
       * Return true if both of the term's expressions
       * apply to the speicified schema.
       */
      public boolean appliesTo(Schema sch) {
        return lhs.appliesTo(sch) && rhs.appliesTo(sch);
      }

      public String toString() {
        return lhs.toString() + "=" + rhs.toString();
      }
    }
    ```
1. Create `query/Predicate.java`
    ```java
    package simpledb.query;

    import java.util.ArrayList;
    import java.util.Iterator;
    import java.util.List;

    /*
     * Predicate is a Boolean combination of terms.
     */
    public class Predicate {
      private List<Term> terms = new ArrayList<>();

      /*
       * Create an empty predicate, corresponding to "true".
       */
      public Predicate() {
      }

      /*
       * Create a predicate containing a single term
       */
      public Predicate(Term t) {
        terms.add(t);
      }

      /*
       * Return true if all the terms are satisfied
       * with the specified scan.
       */
      public boolean isSatisfied(Scan s) {
        for (Term t : terms)
          if (!t.isSatisfied(s))
            return false;
        return true;
      }

      public String toString() {
        Iterator<Term> iter = terms.iterator();
        if (!iter.hasNext())
          return "";
        String result = iter.next().toString();
        while (iter.hasNext())
          result += " and " + iter.next().toString();
        return result;
      }

    }
    ```
1. Create `query/SelectScan.java`
    ```java
    package simpledb.query;

    import simpledb.record.RID;

    /*
     * Select ralational algebra operator.
     * All methods except nect delegate their work to the underlying scan.
     */
    public class SelectScan implements UpdateScan {
      private Scan s;
      private Predicate pred;

      public SelectScan(Scan s, Predicate pred) {
        this.s = s;
        this.pred = pred;
      }

      @Override
      public void beforeFirst() {
        s.beforeFirst();
      }

      @Override
      public boolean next() {
        while (s.next())
          if (pred.isSatisfied(s))
            return true;
        return false;
      }

      @Override
      public int getInt(String fldname) {
        return s.getInt(fldname);
      }

      @Override
      public String getString(String fldname) {
        return s.getString(fldname);
      }

      @Override
      public Constant getVal(String fldname) {
        return s.getVal(fldname);
      }

      @Override
      public boolean hasField(String fldname) {
        return s.hasField(fldname);
      }

      @Override
      public void close() {
        s.close();
      }

      @Override
      public void setVal(String fldname, Constant val) {
        UpdateScan us = (UpdateScan) s;
        us.setVal(fldname, val);
      }

      @Override
      public void setInt(String fldname, int val) {
        UpdateScan us = (UpdateScan) s;
        us.setInt(fldname, val);
      }

      @Override
      public void setString(String fldname, String val) {
        UpdateScan us = (UpdateScan) s;
        us.setString(fldname, val);
      }

      @Override
      public void insert() {
        UpdateScan us = (UpdateScan) s;
        us.insert();
      }

      @Override
      public void delete() {
        UpdateScan us = (UpdateScan) s;
        us.delete();
      }

      @Override
      public RID getRid() {
        UpdateScan us = (UpdateScan) s;
        return us.getRid();
      }

      @Override
      public void moveToRid(RID rid) {
        UpdateScan us = (UpdateScan) s;
        us.moveToRid(rid);
      }
    }
    ```
1. Add the following codes to `App.java`

    ```java
    // 8. Query Processing
    System.out.println("8.1. SelectScan -------------");
    tx = new Transaction(fm, lm, bm);
    // Schema for T1
    Schema sch1 = new Schema();
    sch1.addIntField("A");
    sch1.addStringField("B", 9);
    Layout layout1 = new Layout(sch1);

    // UpdateScan: insert random data to table T1
    UpdateScan s1 = new TableScan(tx, "T1", layout1);
    s1.beforeFirst();
    int n = 10;
    System.out.println("Inserting " + n + " random records into T1.");
    for (int i = 0; i < n; i++) {
      s1.insert();
      int k = (int) Math.round(Math.random() * 50);
      s1.setInt("A", k);
      s1.setString("B", "rec" + k);
    }
    s1.close();

    // TableScan of T1
    Scan s2 = new TableScan(tx, "T1", layout1);

    // SelectScan
    Constant c = new Constant(10);
    Term t = new Term(new Expression("A"), new Expression(c)); // where A = 10
    Predicate pred = new Predicate(t);
    System.out.println("The predicate is " + pred);
    Scan s3 = new SelectScan(s2, pred);

    while (s3.next())
      System.out.println("A: " + s3.getInt("A") + ", B: " + s3.getString("B"));

    s3.close();
    tx.commit();
    ```
1. Run

    ```
    ./gradlew run
    ```

### 8.2. ProjectScan

1. Add `query/ProjectScan.java`

    ```java
    package simpledb.query;

    import java.util.List;

    public class ProjectScan implements Scan {
      private Scan s;
      private List<String> fieldlist;

      public ProjectScan(Scan s, List<String> fieldlist) {
        this.s = s;
        this.fieldlist = fieldlist;
      }

      @Override
      public void beforeFirst() {
        s.beforeFirst();
      }

      @Override
      public boolean next() {
        return s.next();
      }

      @Override
      public int getInt(String fldname) {
        if (hasField(fldname))
          return s.getInt(fldname);
        else
          throw new RuntimeException("field " + fldname + " not found.");
      }

      @Override
      public String getString(String fldname) {
        if (hasField(fldname))
          return s.getString(fldname);
        else
          throw new RuntimeException("field " + fldname + " not found.");
      }

      @Override
      public Constant getVal(String fldname) {
        if (hasField(fldname))
          return s.getVal(fldname);
        else
          throw new RuntimeException("field " + fldname + " not found.");
      }

      @Override
      public boolean hasField(String fldname) {
        return fieldlist.contains(fldname);
      }

      @Override
      public void close() {
        s.close();
      }
    }
    ```

1. Add the following to `App.java`

    ```java
    System.out.println("8.2. ProjectScan");
    // ProjectScan
    List<String> fields = Arrays.asList("B");
    Scan s4 = new ProjectScan(s3, fields);
    while (s4.next())
      System.out.println(s4.getString("B"));

    s4.close(); // previously s3.close()
    ```

1. Run

    ```
    ./gradlew run
    ```
### 8.3. ProductScan
1. Add `query/ProductScan.java`

    ```java
    package simpledb.query;
    
    /*
     * The product relational algebra operator
     */
    public class ProductScan implements Scan {
      private Scan s1;
      private Scan s2;
    
      public ProductScan(Scan s1, Scan s2) {
        this.s1 = s1;
        this.s2 = s2;
      }
    
      /*
       * The LHS scan is positioned at its first record, and
       * the RHS scan is positioned before its first record.
       */
      @Override
      public void beforeFirst() {
        s1.beforeFirst();
        s1.next();
        s2.beforeFirst();
      }
    
      /*
       * Move RHS if there's next record in the inner loop,
       * otherwise, move the RHS to the first record and
       * increment the LHS position (outer loop)
       */
      @Override
      public boolean next() {
        if (s2.next())
          return true;
        else {
          s2.beforeFirst();
          return s2.next() && s1.next();
        }
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
      }}
    ```

1. Add the following to `App.java`
    ```java
    System.out.println("8.3. ProjectScan -------------");
    tx = new Transaction(fm, lm, bm);
    // Schema for T2
    sch2 = new Schema();
    sch2.addIntField("C");
    sch2.addStringField("D", 9);
    Layout layout2 = new Layout(sch2);

    // UpdateScan: insert random data to table T1
    ts = new TableScan(tx, "T2", layout2);
    ts.beforeFirst();
    System.out.println("Inserting " + n + " random records into T2.");
    for (int i = 0; i < n; i++) {
      ts.insert();
      ts.setInt("C", n - i - 1);
      ts.setString("D", "rec" + (n - i - 1));
    }
    ts.close();

    Scan ts1 = new TableScan(tx, "T1", layout1);
    Scan ts2 = new TableScan(tx, "T2", layout2);
    Scan ps = new ProductScan(ts1, ts2);
    ps.beforeFirst();
    System.out.println("prepare scans");
    while (ps.next())
      System.out.println("B: " + ps.getString("B") + ", D: " + ps.getString("D"));
    ps.close();
    tx.commit();
    ```
1. Run
    ```
    ./gradlew run
    ```
