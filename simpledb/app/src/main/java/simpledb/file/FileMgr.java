package simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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

  public synchronized void read(String filename, Page p) {
    try {
      RandomAccessFile f = getFile(filename);
      f.seek(0); // TODO: enable to read from the specified position
      f.getChannel().read(p.contents());
    } catch (IOException e) {
      throw new RuntimeException("cannot read file " + filename);
    }
  }

  public synchronized void write(String filename, Page page) {
    try {
      RandomAccessFile f = getFile(filename);
      f.seek(0); // TODO: enable to write from the specified position
      f.getChannel().write(page.contents());
    } catch (IOException e) {
      throw new RuntimeException("cannot write to file " + filename);
    }
  }

  public int blockSize() {
    return blocksize;
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
