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

  public synchronized void read(BlockId blk, Page p) {
    try {
      RandomAccessFile f = getFile(blk.fileName());
      f.seek(blk.number() * blocksize);
      f.getChannel().read(p.contents());
    } catch (IOException e) {
      throw new RuntimeException("cannot read file " + blk.fileName());
    }
  }

  public synchronized void write(BlockId blk, Page page) {
    try {
      RandomAccessFile f = getFile(blk.fileName());
      f.seek(blk.number() * blocksize);
      f.getChannel().write(page.contents());
    } catch (IOException e) {
      throw new RuntimeException("cannot write to file " + blk.fileName());
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
