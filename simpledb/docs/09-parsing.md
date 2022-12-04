## Chapter 9: Parsing

### Overview

![](parser.drawio.svg)

### 9.1. Lexer

1. Create `parse/Lexer.java`
    ```java
    package simpledb.parse;

    import java.io.IOException;
    import java.io.StreamTokenizer;
    import java.io.StringReader;
    import java.util.Arrays;
    import java.util.Collection;

    /*
     * Lexical analyzer supports the five types of tokens:
     * 1. Single-character delimiters
     * 2. Integer constants
     * 3. String constants
     * 4. Keywords
     * 5. Identifiers
     * eatXXX means extract the value, move to next token, and return the value.
     */
    public class Lexer {
      private Collection<String> keywords;
      private StreamTokenizer tok;

      public Lexer(String s) {
        initKeywords();
        tok = new StreamTokenizer(new StringReader(s));
        tok.ordinaryChar('.'); // disallow "." in identifiers
        tok.wordChars('_', '_'); // allow "_" in identifiers
        tok.lowerCaseMode(true); // ids and keywords are converted
        nextToken();
      }

      /*
       * Return true if the current token is
       * the specified delimiter character
       */
      public boolean matchDelim(char d) {
        return d == (char) tok.ttype;
      }

      /*
       * Return true if the current token is an integer
       */
      public boolean matchIntConstant() {
        return tok.ttype == StreamTokenizer.TT_NUMBER;
      }

      /*
       * Return true if the current token is a string.
       */
      public boolean matchStringConstant() {
        return '\'' == (char) tok.ttype;
      }

      /*
       * Return true if the current token is te speccified keyword.
       */
      public boolean matchKeyword(String w) {
        return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
      }

      /*
       * Return true if the current token is a legal identifier.
       */
      public boolean matchId() {
        return tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
      }

      // Methods to "eat" the current token

      /*
       * Move to next token if the current token is the delimiter.
       * Otherwise, throw exception.
       */
      public void eatDelim(char d) {
        if (!matchDelim(d))
          throw new BadSyntaxException();
        nextToken();
      }

      public int eatIntConstant() {
        if (!matchIntConstant())
          throw new BadSyntaxException();
        int i = (int) tok.nval;
        nextToken();
        return i;
      }

      public String eatStringConstant() {
        if (!matchStringConstant())
          throw new BadSyntaxException();
        String s = tok.sval;
        nextToken();
        return s;
      }

      public void eatKeyword(String w) {
        if (!matchKeyword(w))
          throw new BadSyntaxException();
        nextToken();
      }

      public String eatId() {
        if (!matchId())
          throw new BadSyntaxException();
        String s = tok.sval;
        nextToken();
        return s;
      }

      private void nextToken() {
        try {
          tok.nextToken();
        } catch (IOException e) {
          throw new BadSyntaxException();
        }
      }

      private void initKeywords() {
        keywords = Arrays.asList("select", "from", "where", "and",
            "insert", "into", "values", "delete", "update", "set",
            "create", "table", "int", "varchar", "view", "as", "index", "on");
      }
    }
    ```

1. Create `parse/BadSyntaxException.java`

    ```java
    package simpledb.parse;

    @SuppressWarnings("serial")
    public class BadSyntaxException extends RuntimeException {

    }
    ```
1. Add test `parse/LexerTest.java`

    ```java
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
    }
    ```

### 9.2. Parser 1: Methods for parsing predicates, terms, expressions, constants, and fields

1. Create `parse/Parser.java`

    ```java
    package simpledb.parse;

    import simpledb.query.Constant;
    import simpledb.query.Expression;
    import simpledb.query.Predicate;
    import simpledb.query.Term;

    public class Parser {
      private Lexer lex;

      public Parser(String s) {
        lex = new Lexer(s);
      }

      // Methods for parsing predicates, terms, expressions, constants, and fields

      /*
       * move to next token and return the id
       */
      public String field() {
        return lex.eatId();
      }

      /*
       * Return new constant with string if match a string constant.
       * Otherwise return new constant with an integer.
       */
      public Constant constant() {
        if (lex.matchStringConstant())
          return new Constant(lex.eatStringConstant());
        else
          return new Constant(lex.eatIntConstant());
      }

      /*
       * Return new expression based on matchedId
       * use fields If matches id, otherwise constant
       */
      public Expression expression() {
        if (lex.matchId())
          return new Expression(field());
        else
          return new Expression(constant());
      }

      /*
       * term only supports equality comparison
       */
      public Term term() {
        Expression lhs = expression();
        lex.eatDelim('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
      }

      public Predicate predicate() {
        Predicate pred = new Predicate(term());
        if (lex.matchKeyword("and")) {
          lex.eatKeyword("and");
          pred.conjoinWith(predicate());
        }
        return pred;
      }
    }
    ```

1. Add conjoinWith to `query/Predicate.java`

    ```java
    public void conjoinWith(Predicate pred) {
      terms.addAll(pred.terms);
    }
    ```

1. Add `parse/ParserTest.java`

    ```java
    package simpledb.parse;

    import static org.junit.jupiter.api.Assertions.assertEquals;
    import static org.junit.jupiter.api.Assertions.assertFalse;
    import static org.junit.jupiter.api.Assertions.assertTrue;

    import org.junit.jupiter.api.Test;

    import simpledb.query.Constant;
    import simpledb.query.Expression;
    import simpledb.query.Predicate;
    import simpledb.query.Term;

    public class ParserTest {
      @Test
      public void testParseField() {
        String s = "a";
        Parser p = new Parser(s);
        String field = p.field();
        assertEquals("a", field);
      }

      @Test
      public void testParseConstantInt() {
        String s = "10";
        Parser p = new Parser(s);
        Constant cons = p.constant();
        assertEquals(10, cons.asInt());
      }

      @Test
      public void testParseConstantString() {
        String s = "'test'";
        Parser p = new Parser(s);
        Constant cons = p.constant();
        assertEquals("test", cons.asString());
      }

      @Test
      public void testParseExpressionField() {
        String s = "a";
        Parser p = new Parser(s);
        Expression exp = p.expression();
        assertTrue(exp.isFieldName()); // 'a' is a field
        assertEquals("a", exp.asFieldName()); // 'a' as field name
      }

      @Test
      public void testParseExpressionConstantString() {
        String s = "'test'";
        Parser p = new Parser(s);
        Expression exp = p.expression();
        assertFalse(exp.isFieldName());
        assertEquals(new Constant("test"), exp.asConstant());
      }

      @Test
      public void testParseExpressionConstantInt() {
        String s = "10";
        Parser p = new Parser(s);
        Expression exp = p.expression();
        assertFalse(exp.isFieldName());
        assertEquals(new Constant(10), exp.asConstant());
      }

      @Test
      public void testParseTerm() {
        String s = "a = 10";
        Parser p = new Parser(s);
        Term term = p.term();
        assertEquals("a=10", term.toString());
      }

      @Test
      public void testParsePredicate() {
        String s = "a = 10 AND b = 'test'";
        Parser p = new Parser(s);
        Predicate pred = p.predicate();
        assertEquals("a=10 and b=test", pred.toString());
      }
    }
    ```

1. Run test
    ```
    ./gradlew test
    ```
