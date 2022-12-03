package simpledb.parse;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

/*
 * Lexical analyzer encapsulates StreamTokenizer
 * that supports the five types of tokens:
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
