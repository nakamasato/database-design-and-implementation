package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
  private ByteBuffer bb;
  public static Charset CHARSET = StandardCharsets.US_ASCII;

  // for data buffer
  public Page(int blocksize) {
    bb = ByteBuffer.allocateDirect(blocksize);
  }

  // for log pages
  public Page(byte[] b) {
    bb = ByteBuffer.wrap(b);
  }

  public int getInt(int offset) {
    return bb.getInt(offset);
  }

  public void setInt(int offset, int n) {
    bb.putInt(offset, n);
  }

  public byte[] getBytes(int offset) {
    bb.position(offset);
    int length = bb.getInt();
    byte[] b = new byte[length];
    bb.get(b);
    return b;
  }

  public void setBytes(int offset, byte[] b) {
    bb.position(offset);
    bb.putInt(b.length);
    bb.put(b);
  }

  public String getString(int offset) {
    byte[] b = getBytes(offset);
    return new String(b, CHARSET);
  }

  public void setString(int offset, String s) {
    byte[] b = s.getBytes(CHARSET);
    setBytes(offset, b);
  }

  public static int maxLength(int strlen) {
    float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
    return Integer.BYTES + (strlen * (int) bytesPerChar);
  }

  ByteBuffer contents() {
    bb.position(0);
    return bb;
  }
}
