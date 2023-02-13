package simpledb.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import simpledb.record.Schema;

@ExtendWith(MockitoExtension.class)
public class PredicateTest {

  /*
   * Term: Schema.fld1 = Schema.fld2
   * selectSubPred(): Predicate(term)
   */
  @Test
  public void testSelectSubPredTrue() {
    Schema sch = new Schema();
    sch.addIntField("fld1");
    sch.addIntField("fld2");
    Term t = new Term(new Expression("fld1"), new Expression("fld2"));
    Predicate pred = new Predicate(t);
    Predicate selectpred = pred.selectSubPred(sch);
    assertNotNull(selectpred);
    assertEquals("fld1=fld2", selectpred.toString());
    assertEquals("fld2", selectpred.equatesWithField("fld1"));
  }

  /*
   * Term: Schema.fld1 = OtherSchema.fld2
   * selectSubPred(): null
   */
  @Test
  public void testSelectSubPredFalseIfCompareWithOtherSchema() {
    Schema sch = new Schema();
    sch.addIntField("fld1");
    Term t = new Term(new Expression("fld1"), new Expression("fld2"));
    Predicate pred = new Predicate(t);
    Predicate selectpred = pred.selectSubPred(sch);
    assertNull(selectpred);
  }

  /*
   * Term: Schema.fld1 = Constant
   * selectSubPred(): null
   */
  @Test
  public void testSelectSubPredFalseIfCompareFieldInSchemaWithConstant() {
    Schema sch = new Schema();
    sch.addIntField("fld1");
    Term t = new Term(new Expression("fld1"), new Expression(new Constant("val")));
    Predicate pred = new Predicate(t);
    Predicate selectpred = pred.selectSubPred(sch);
    assertNotNull(selectpred);
    assertEquals("fld1=val", selectpred.toString());
    assertEquals(new Constant("val"), selectpred.equatesWithConstant("fld1"));
  }

  /*
   * Term: OtherSchema.fld2 = Constant
   * selectSubPred(): null
   */
  @Test
  public void testSelectSubPredFalseIfCompareWithFieldNotInTheSchema() {
    Schema sch = new Schema();
    sch.addIntField("fld1");
    Term t = new Term(new Expression("fld2"), new Expression(new Constant("val")));
    Predicate pred = new Predicate(t);
    Predicate selectpred = pred.selectSubPred(sch);
    assertNull(selectpred);
  }

  /*
   * Term: sch1.fld1 = sch2.fld2
   * pred.joinSubPred(sch1, sch2): predicate(term)
   */
  @Test
  public void testJoinSubPredTrue() {
    Schema sch1 = new Schema();
    sch1.addIntField("fld1");
    Schema sch2 = new Schema();
    sch2.addIntField("fld2");
    Term t = new Term(new Expression("fld1"), new Expression("fld2"));
    Predicate pred = new Predicate(t);
    Predicate joinpred = pred.joinSubPred(sch1, sch2);
    assertNotNull(joinpred);
    assertEquals("fld1=fld2", joinpred.toString());
    assertEquals("fld2", joinpred.equatesWithField("fld1"));
  }

  /*
   * Term: sch1.fld1 = sch1.fld2
   * pred.joinSubPred(sch1, sch2): null
   */
  @Test
  public void testJoinSubPredFalseIfTermOnlyAppliedToOneSchema() {
    Schema sch1 = new Schema();
    sch1.addIntField("fld1");
    sch1.addIntField("fld2");
    Schema sch2 = new Schema();
    Term t = new Term(new Expression("fld1"), new Expression("fld2"));
    Predicate pred = new Predicate(t);
    Predicate joinpred = pred.joinSubPred(sch1, sch2);
    assertNull(joinpred);
  }
}
