package simpledb.opt;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

@ExtendWith(MockitoExtension.class)
public class TablePlannerTest {
  @Mock
  Transaction tx;

  @Mock
  MetadataMgr mdm;

  /*
   * makeSelectPlan() with no index nor pred
   * makeSelectPlan: TablePlan
   */
  @Test
  public void testMakeSelectPlanWithNoIndexNorPred() {
    Predicate pred = new Predicate();
    when(mdm.getIndexInfo("test", tx)).thenReturn(new HashMap<>());
    when(mdm.getLayout("test", tx)).thenReturn(new Layout(new Schema()));
    TablePlanner tp = new TablePlanner("test", pred, tx, mdm);
    assertInstanceOf(TablePlan.class, tp.makeSelectPlan());
  }

  /*
   * makeSelectPlan() with no index with Predicate(fld1=C)
   * makeSelectPlan: SelectPlan
   */
  @Test
  public void testMakeSelectPlanWithNoIndexWithPredicateFieldConstant() {

    Schema sch = new Schema();
    sch.addStringField("fld1", 10);
    sch.addStringField("fld2", 10);
    Predicate pred = new Predicate(new Term(new Expression("fld1"), new Expression(new Constant("val"))));
    when(mdm.getIndexInfo("test", tx)).thenReturn(new HashMap<>());
    when(mdm.getLayout("test", tx)).thenReturn(new Layout(sch));
    TablePlanner tp = new TablePlanner("test", pred, tx, mdm);
    assertInstanceOf(SelectPlan.class, tp.makeSelectPlan());
  }

  /*
   * makeSelectPlan() with no index with Predicate(fld1=fld2)
   * makeSelectPlan: SelectPlan
   */
  @Test
  public void testMakeSelectPlanWithNoIndexWithPredicate() {

    Schema sch = new Schema();
    sch.addStringField("fld1", 10);
    sch.addStringField("fld2", 10);
    Predicate pred = new Predicate(new Term(new Expression("fld1"), new Expression("fld2")));
    when(mdm.getIndexInfo("test", tx)).thenReturn(new HashMap<>());
    when(mdm.getLayout("test", tx)).thenReturn(new Layout(sch));
    TablePlanner tp = new TablePlanner("test", pred, tx, mdm);
    assertInstanceOf(SelectPlan.class, tp.makeSelectPlan());
  }

  /*
   * makeSelectPlan() with index on fld1 with Predicate(fld1=C)
   * makeSelectPlan: SelectPlan(IndexSelectPlan)
   */
  @Test
  public void testMakeSelectPlanWithIndexOnFld1AndPredicate() {

    Schema sch = new Schema();
    sch.addStringField("fld1", 10);
    Predicate pred = new Predicate(new Term(new Expression("fld1"), new Expression(new Constant("val"))));
    Map<String, IndexInfo> indexInfo = new HashMap<>() {
      {
        put("fld1", new IndexInfo("fld1_idx", "fld1", sch, tx, null));
      }
    };
    when(mdm.getIndexInfo("test", tx)).thenReturn(indexInfo);
    when(mdm.getLayout("test", tx)).thenReturn(new Layout(sch));
    TablePlanner tp = new TablePlanner("test", pred, tx, mdm);
    assertInstanceOf(SelectPlan.class, tp.makeSelectPlan());
  }

  /*
   * makeSelectPlan() with index on fld1 with no Predicate
   * makeSelectPlan: TablePlan
   */
  @Test
  public void testMakeSelectPlanWithIndexOnFld1AndNoPredicate() {

    Schema sch = new Schema();
    sch.addStringField("fld1", 10);
    Predicate pred = new Predicate();
    Map<String, IndexInfo> indexInfo = new HashMap<>() {
      {
        put("fld1", new IndexInfo("fld1_idx", "fld1", sch, tx, null));
      }
    };
    when(mdm.getIndexInfo("test", tx)).thenReturn(indexInfo);
    when(mdm.getLayout("test", tx)).thenReturn(new Layout(sch));
    TablePlanner tp = new TablePlanner("test", pred, tx, mdm);
    assertInstanceOf(TablePlan.class, tp.makeSelectPlan());
  }

  /*
   * TablePlanner(table: "test", pred: fld1=fld2, mdm) without index
   * tp.makejoinPlan() with TablePlan(table: "test2")
   * myschema: test.fld1
   * currentschema: test2.fld2
   * makeSelectPlan: SelectPlan
   */
  @Test
  public void testMakeJoinPlanProductJoin() {

    Schema sch1 = new Schema();
    sch1.addStringField("fld1", 10);
    Schema sch2 = new Schema();
    sch2.addStringField("fld2", 10);
    Predicate pred = new Predicate(new Term(new Expression("fld1"), new Expression("fld2")));
    Map<String, IndexInfo> indexInfo = new HashMap<>();
    when(mdm.getIndexInfo("test", tx)).thenReturn(indexInfo);
    when(mdm.getLayout("test", tx)).thenReturn(new Layout(sch1));
    when(mdm.getLayout("test2", tx)).thenReturn(new Layout(sch2));
    TablePlanner tp = new TablePlanner("test", pred, tx, mdm);
    Plan plan = new TablePlan(tx, "test2", mdm);
    assertNotNull(tp.makeJoinPlan(plan));
    assertInstanceOf(SelectPlan.class, tp.makeJoinPlan(plan));
  }

  /*
   * TablePlanner(table: "test", pred: fld1=fld2, mdm) with index on fld1
   * tp.makejoinPlan() with TablePlan(table: "test2")
   * myschema: test.fld1
   * currentschema: test2.fld2
   * makeSelectPlan: SelectPlan
   */
  @Test
  public void testMakeJoinPlanIndexJoin() {

    Schema sch1 = new Schema();
    sch1.addStringField("fld1", 10);
    Schema sch2 = new Schema();
    sch2.addStringField("fld2", 10);
    Predicate pred = new Predicate(new Term(new Expression("fld1"), new Expression("fld2")));
    Map<String, IndexInfo> indexInfo = new HashMap<>() {
      {
        put("fld1", new IndexInfo("fld1_idx", "fld1", sch1, tx, null));
      }
    };
    when(mdm.getIndexInfo("test", tx)).thenReturn(indexInfo);
    when(mdm.getLayout("test", tx)).thenReturn(new Layout(sch1));
    when(mdm.getLayout("test2", tx)).thenReturn(new Layout(sch2));
    TablePlanner tp = new TablePlanner("test", pred, tx, mdm);
    Plan plan = new TablePlan(tx, "test2", mdm);
    assertNotNull(tp.makeJoinPlan(plan));
    assertInstanceOf(SelectPlan.class, tp.makeJoinPlan(plan));
  }

  /*
   * Case: No joinpred exists -> null
   */
  @Test
  public void testMakeJoinPlanNull() {
    Schema sch1 = new Schema();
    sch1.addStringField("fld1", 10);
    Schema sch2 = new Schema();
    Predicate pred = new Predicate(new Term(new Expression("fld1"), new Expression(new Constant("val"))));
    Map<String, IndexInfo> indexInfo = new HashMap<>();
    when(mdm.getIndexInfo("test", tx)).thenReturn(indexInfo);
    when(mdm.getLayout("test", tx)).thenReturn(new Layout(sch1));
    when(mdm.getLayout("test2", tx)).thenReturn(new Layout(sch2));
    TablePlanner tp = new TablePlanner("test", pred, tx, mdm);
    Plan plan = new TablePlan(tx, "test2", mdm);
    assertNull(tp.makeJoinPlan(plan));
  }

  /*
   * TablePlanner(table: "test", pred: fld1=fld2, mdm)
   * tp.makeProductPlan(TablePlan(table: "test2")):
   * MultiBufferProductPlan(TablePlan("test2"), SelectPlan + pred)
   */
  @Test
  public void testMakeProductPlan() {

    Schema sch1 = new Schema();
    sch1.addStringField("fld1", 10);
    Schema sch2 = new Schema();
    sch2.addStringField("fld2", 10);
    Predicate pred = new Predicate(new Term(new Expression("fld1"), new Expression("fld2")));
    Map<String, IndexInfo> indexInfo = new HashMap<>();
    when(mdm.getIndexInfo("test", tx)).thenReturn(indexInfo);
    when(mdm.getLayout("test", tx)).thenReturn(new Layout(sch1));
    when(mdm.getLayout("test2", tx)).thenReturn(new Layout(sch2));
    TablePlanner tp = new TablePlanner("test", pred, tx, mdm);
    Plan plan = new TablePlan(tx, "test2", mdm);
    assertInstanceOf(MultibufferProductPlan.class, tp.makeProductPlan(plan));
  }
}
