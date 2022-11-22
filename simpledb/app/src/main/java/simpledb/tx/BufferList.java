package simpledb.tx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;

public class BufferList {
  private Map<BlockId, Buffer> buffers = new HashMap<>();
  private List<BlockId> pins = new ArrayList<>();
  private BufferMgr bm;

  public BufferList(BufferMgr bm) {
    this.bm = bm;
  }

  Buffer getBuffer(BlockId blk) {
    return buffers.get(blk);
  }

  void pin(BlockId blk) {
    Buffer buff = bm.pin(blk);
    buffers.put(blk, buff);
    pins.add(blk);
  }

  void unpin(BlockId blk) {
    Buffer buff = buffers.get(blk);
    bm.unpin(buff);
    pins.remove(blk);
    if (!pins.contains(blk))
      buffers.remove(blk);
  }

  void unpinAll() {
    for (BlockId blk : pins) {
      Buffer buff = buffers.get(blk);
      bm.unpin(buff);
    }
    buffers.clear();
    pins.clear();
  }
}
