package simpledb.tx.recovery;

import simpledb.file.Page;
import simpledb.tx.Transaction;

public interface LogRecord {
  static final int CHECKPOINT = 0;
  static final int START = 1;
  static final int COMMIT = 2;
  static final int ROLLBACK = 3;
  static final int SETINT = 4;
  static final int SETSTRING = 5;

  int op();

  int txNumber();

  void undo(Transaction tx);

  static LogRecord createLogRecord(byte[] bytes) {
    Page p = new Page(bytes);
    switch (p.getInt(0)) {
      case CHECKPOINT:
        return new CheckpointRecord();
      case COMMIT:
        return new CommitRecord(p);
      case ROLLBACK:
        return new RollbackRecord(p);
      case SETINT:
        return new SetIntRecord(p);
      case SETSTRING:
        return new SetStringRecord(p);
      case START:
        return new StartRecord(p);
      default:
        System.out.println("LogRecord p.getInt: " + p.getInt(0));
        return null;
    }
  }
}
