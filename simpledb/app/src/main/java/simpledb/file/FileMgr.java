package simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class FileMgr {
  private File dbDirectory;
  private int blocksize;
  private boolean isNew;
  private Map<String, RandomAccessFile> openFiles = new HashMap<>();
  public static Charset CHARSET = StandardCharsets.US_ASCII;

  public FileMgr(File dbDirectory, int blocksize) {
    this.dbDirectory = dbDirectory;
    this.blocksize = blocksize;
    isNew = !dbDirectory.exists();

    // create the directory if not exists
    if (isNew)
      dbDirectory.mkdirs();

    // remove any leftover temporary tables
    for (String filename : dbDirectory.list())
      if (filename.startsWith("temp"))
        new File(dbDirectory, filename).delete();
  }

  public synchronized void read(String filename) {
    try {
      RandomAccessFile f = getFile(filename);
      f.seek(0); // TODO: enable to read from the specified position
      ByteBuffer bb = ByteBuffer.allocateDirect(blocksize);
      bb.position(0); // [B@8bcc55f
      int readBytes = f.getChannel().read(bb);
      System.out.println("readBytes: " + readBytes);

      // getBytes
      bb.position(0);
      int length = bb.getInt(); // get first int that indicates the length to read
      System.out.println("lenth:" + length);
      byte[] b = new byte[length];
      bb.get(b);
      System.out.println(b);

      // get String from []bytes
      String msg = new String(b, CHARSET); // TODO: enable to read Int and String
      System.out.println("read message from file: " + msg);
    } catch (IOException e) {
      throw new RuntimeException("cannot read file " + filename);
    }
  }

  public synchronized void write(String filename) {
    try {
      RandomAccessFile f = getFile(filename);
      f.seek(0); // TODO: enable to write from the specified position
      String msg = "test"; // TODO: enable to pass the contents
      System.out.println("write message: " + msg);
      byte[] b = msg.getBytes(CHARSET);

      // setBytes
      ByteBuffer bb = ByteBuffer.allocateDirect(blocksize);
      bb.position(0);
      bb.putInt(b.length); // put length before the content
      bb.put(b); // put the content
      bb.position(0);
      f.getChannel().write(bb);
    } catch (IOException e) {
      throw new RuntimeException("cannot write to file " + filename);
    }
  }

  private RandomAccessFile getFile(String filename) throws IOException {
    RandomAccessFile f = openFiles.get(filename);
    if (f == null) {
      File dbTable = new File(dbDirectory, filename);
      f = new RandomAccessFile(dbTable, "rws");
      openFiles.put(filename, f);
    }
    return f;
  }
}
