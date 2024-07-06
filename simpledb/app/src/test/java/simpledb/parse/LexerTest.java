package simpledb.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class LexerTest {
  @Test
  public void testIdEqualsInt() {
    String s = "a = 10";
    Lexer lex = new Lexer(s);

    assertTrue(lex.matchId());
    String x = lex.eatId();
    lex.eatDelim('=');
    int y = lex.eatIntConstant();
    assertEquals("a", x);
    assertEquals(10, y);
  }

  @Test
  public void testIntEqualsId() {
    String s = "10 = a";
    Lexer lex = new Lexer(s);

    assertFalse(lex.matchId());
    int x = lex.eatIntConstant();
    lex.eatDelim('=');
    String y = lex.eatId();
    assertEquals(10, x);
    assertEquals("a", y);
  }

  @Test
  public void testLowerCaseMode() {
    String s = "10 = A";
    Lexer lex = new Lexer(s);

    assertFalse(lex.matchId());
    int x = lex.eatIntConstant();
    lex.eatDelim('=');
    String y = lex.eatId();
    assertEquals(10, x);
    assertEquals("a", y);
  }
}
