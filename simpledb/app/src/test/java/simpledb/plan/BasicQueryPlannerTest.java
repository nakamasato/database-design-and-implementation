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
import simpledb.parse.Parser;
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
   * Test case of SQL: select fld1 from tbl1 where fld1 = 1 ProjectPlan(SelectPlan(TablePlan(tx,
   * "tbl1", mdm), pred), fields)
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
   * ProjectPlan(SelectPlan(Product(TablePlan(tx, "tbl1", mdm), TablePlan(tx, "tbl2", mdm)), pred),
   * fields)
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
    QueryData qd =
        new QueryData(Arrays.asList("fld1", "fld2"), Arrays.asList("tbl1", "tbl2"), pred);
    Plan plan = basicQueryPlanner.createPlan(qd, null);

    assertTrue(plan instanceof ProjectPlan);
    assertEquals(2, plan.schema().fields().size()); // fld1, fld2
    assertEquals(3010, plan.blockAccessed()); // ProductPlan.blockaccessed() = B(t1) + R(t1) * B(t2)
                                              // = 10 + 100 * 30

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

  /*
   * Chapter 15.2. Test case of SQL: select Grade from STUDENT, SECTION, ENROLL where SId=StudentId
   * and SectId=SectionId and SName='joe' and YearOffered=2020
   */
  @Test
  public void test() {
    Schema studentSch = new Schema();
    studentSch.addIntField("Sid");
    studentSch.addStringField("SName", 10);
    studentSch.addIntField("MajorId");
    studentSch.addIntField("GradYear");
    Layout studentLayout = new Layout(studentSch);

    Schema enrollSch = new Schema();
    enrollSch.addIntField("Eid");
    enrollSch.addIntField("StudentId");
    enrollSch.addIntField("SectionId");
    enrollSch.addIntField("grade");
    Layout enrollLayout = new Layout(enrollSch);

    Schema sectionSch = new Schema();
    sectionSch.addIntField("SectId");
    sectionSch.addIntField("CourseId");
    sectionSch.addStringField("Prof", 8);
    sectionSch.addIntField("Year");
    Layout sectionLayout = new Layout(sectionSch);

    Parser parser = new Parser(
        "select Grade from Student, Section, Enroll where Sid=StudentId and SectId=Sectionid and SName='joe' and Year=2020");
    QueryData data = parser.query();

    StatInfo studentStatInfo = new StatInfo(4500, 45000);
    when(mdm.getViewDef("student", null)).thenReturn(null);
    when(mdm.getLayout("student", null)).thenReturn(studentLayout);
    when(mdm.getStatInfo("student", studentLayout, null)).thenReturn(studentStatInfo);

    StatInfo enrollStatInfo = new StatInfo(50000, 1500000);
    when(mdm.getViewDef("enroll", null)).thenReturn(null);
    when(mdm.getLayout("enroll", null)).thenReturn(enrollLayout);
    when(mdm.getStatInfo("enroll", enrollLayout, null)).thenReturn(enrollStatInfo);

    StatInfo sectionStatInfo = new StatInfo(2500, 25000);
    when(mdm.getViewDef("section", null)).thenReturn(null);
    when(mdm.getLayout("section", null)).thenReturn(sectionLayout);
    when(mdm.getStatInfo("section", sectionLayout, null)).thenReturn(sectionStatInfo);

    BasicQueryPlanner basicQueryPlanner = new BasicQueryPlanner(mdm);
    Plan plan = basicQueryPlanner.createPlan(data, null);

    TablePlan tp = new TablePlan(null, "student", mdm);
    assertEquals(4500, tp.blockAccessed());

    tp = new TablePlan(null, "enroll", mdm);
    assertEquals(50000, tp.blockAccessed());

    tp = new TablePlan(null, "section", mdm);
    assertEquals(2500, tp.blockAccessed());

    assertTrue(plan instanceof ProjectPlan);
    assertEquals(1, plan.schema().fields().size()); // grade
    assertEquals(-1074171212, plan.blockAccessed()); // TODO: how to calculate
    // p1.blockAccessed() + p1.recordsOutput() * p2.blockAccessed();
    assertEquals(3, plan.recordsOutput()); // TODO: how to calculate
  }
}
