## Chapter 12: Indexing

### 12.1. B-Tree Index

1. Create `BTPage`
    ```java
    package simpledb.index.btree;

    import static java.sql.Types.INTEGER;

    import simpledb.file.BlockId;
    import simpledb.query.Constant;
    import simpledb.record.Layout;
    import simpledb.record.RID;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    /*
     * BTPage provides the common logic for BTreeLeaf and BTreeDir:
     * 1. store records in sorted order
     * 2. split a block when it's full
     */
    public class BTPage {
      private static String FLD_NAME_DATAVAL = "dataval";
      private static String FLD_NAME_BLOCK = "block";
      private static String FLD_NAME_ID = "id";
      private Transaction tx;
      private BlockId currentblk;
      private Layout layout;

      public BTPage(Transaction tx, BlockId currentblk, Layout layout) {
        this.tx = tx;
        this.currentblk = currentblk;
        this.layout = layout;
        tx.pin(currentblk);
      }

      /*
       * Return the slot position where the first record having the specified
       * search key should be.
       */
      public int findSlotBefore(Constant searchkey) {
        int slot = 0;
        while (slot < getNumRecs() && getDataVal(slot).compareTo(searchkey) < 0)
          slot++;
        return slot - 1;
      }

      public void close() {
        if (currentblk != null)
          tx.unpin(currentblk);
        currentblk = null;
      }

      public boolean isFull() {
        return slotpos(getNumRecs() + 1) >= tx.blockSize();
      }

      /*
       * Split the page at the specified position.
       */
      public BlockId split(int splitpos, int flag) {
        BlockId newblk = appendNew(flag);
        BTPage newpage = new BTPage(tx, newblk, layout);
        transferRecs(splitpos, newpage);
        newpage.setFlag(flag);
        newpage.close();
        return newblk;
      }

      public Constant getDataVal(int slot) {
        return getVal(slot, FLD_NAME_DATAVAL);
      }

      public int getFlag() {
        return tx.getInt(currentblk, 0);
      }

      public void setFlag(int val) {
        tx.setInt(currentblk, 0, val, true);
      }

      public BlockId appendNew(int flag) {
        BlockId blk = tx.append(currentblk.fileName());
        tx.pin(blk);
        format(blk, flag);
        return blk;
      }

      public void format(BlockId blk, int flag) {
        tx.setInt(blk, 0, flag, false);
        tx.setInt(blk, Integer.BYTES, 0, false);
        int recsize = layout.slotSize();
        for (int pos = 2 * Integer.BYTES; pos + recsize <= tx.blockSize(); pos += recsize)
          makeDefaultRecord(blk, pos);
      }

      private void makeDefaultRecord(BlockId blk, int pos) {
        for (String fldname : layout.schema().fields()) {
          int offset = layout.offset(fldname);
          if (layout.schema().type(fldname) == INTEGER)
            tx.setInt(blk, pos + offset, 0, false);
          else
            tx.setString(blk, pos + offset, "", false);
        }
      }

      // Methods called only by BTreeDir

      /*
       * Return the block number in the index record
       * at the specified slot
       */
      public int getChildNum(int slot) {
        return getInt(slot, FLD_NAME_BLOCK);
      }

      /*
       * Insert a directory at the specified slot.
       */
      public void insertDir(int slot, Constant val, int blknum) {
        insert(slot);
        setVal(slot, FLD_NAME_DATAVAL, val);
        setInt(slot, FLD_NAME_BLOCK, blknum);
      }

      public RID getDataRid(int slot) {
        return new RID(getInt(slot, FLD_NAME_BLOCK), getInt(slot, FLD_NAME_ID));
      }

      public void insertLeaf(int slot, Constant val, RID rid) {
        insert(slot);
        setVal(slot, FLD_NAME_DATAVAL, val);
        setInt(slot, FLD_NAME_BLOCK, rid.blockNumber());
        setInt(slot, FLD_NAME_ID, rid.slot());
      }

      public void delete(int slot) {
        for (int i = slot + 1; i < getNumRecs(); i++)
          copyRecord(i, i - 1);
        setNumRecs(getNumRecs() - 1);
      }

      /*
       * Return the number of index records in this page.
       */
      public int getNumRecs() {
        return tx.getInt(currentblk, Integer.BYTES);
      }

      private int getInt(int slot, String fldname) {
        int pos = fldpos(slot, fldname);
        return tx.getInt(currentblk, pos);
      }

      private String getString(int slot, String fldname) {
        int pos = fldpos(slot, fldname);
        return tx.getString(currentblk, pos);
      }

      private Constant getVal(int slot, String fldname) {
        int type = layout.schema().type(fldname);
        if (type == INTEGER)
          return new Constant(getInt(slot, fldname));
        else
          return new Constant(getString(slot, fldname));
      }

      private void setInt(int slot, String fldname, int val) {
        int pos = fldpos(slot, fldname);
        tx.setInt(currentblk, pos, val, true);
      }

      private void setString(int slot, String fldname, String val) {
        int pos = fldpos(slot, fldname);
        tx.setString(currentblk, pos, val, true);
      }

      private void setVal(int slot, String fldname, Constant val) {
        int type = layout.schema().type(fldname);
        if (type == INTEGER)
          setInt(slot, fldname, val.asInt());
        else
          setString(slot, fldname, val.asString());
      }

      private void setNumRecs(int n) {
        tx.setInt(currentblk, Integer.BYTES, n, true);
      }

      private void insert(int slot) {
        for (int i = getNumRecs(); i > slot; i--)
          copyRecord(i - 1, i);
        setNumRecs(getNumRecs() + 1);
      }

      private void copyRecord(int from, int to) {
        Schema sch = layout.schema();
        for (String fldname : sch.fields())
          setVal(to, fldname, getVal(from, fldname));
      }

      private void transferRecs(int slot, BTPage dest) {
        int destslot = 0;
        while (slot < getNumRecs()) {
          dest.insert(destslot);
          Schema sch = layout.schema();
          for (String fldname : sch.fields())
            dest.setVal(destslot, fldname, getVal(slot, fldname));
          delete(slot);
          destslot++;
        }
      }

      private int fldpos(int slot, String fldname) {
        int offset = layout.offset(fldname);
        return slotpos(slot) + offset;
      }

      private int slotpos(int slot) {
        int slotsize = layout.slotSize();
        return Integer.BYTES + Integer.BYTES + (slot * slotsize);
      }
    }
    ```

    1. Block structure: `<flag><number of records><slot 1><slot 2>....`
    1. `flag` stores different values for `BTreeDir` and `BTreeLeaf`.
    1. `insert(slot)` moves all the subsequent records to right by one slot.
    1. `delete(slot)` moves all the subsequent records to left by one slot.
    1. `transferRecs` transfers all the records after the specified slot to the destination BTPage.
    1. `split`: create a new block and BTPage with new block, transfer the records after the specified slot position to the new BTPage, close the new page and return the new block.
1. Create `DirEntry`
    ```java
    package simpledb.index.btree;

    import simpledb.query.Constant;

    /*
     * A directory entry has two components:
     * 1. the block number of the child block
     * 2. the dataval of the first record in the block
     */
    public class DirEntry {
      private Constant dataval; // dataval of the first record
      private int blocknum; // child block

      public DirEntry(Constant dataval, int blocknum) {
        this.dataval = dataval;
        this.blocknum = blocknum;
      }

      public Constant dataval() {
        return dataval;
      }

      public int blockNumber() {
        return blocknum;
      }
    }
    ```
1. Create `BTreeLeaf`
    ```java
    package simpledb.index.btree;

    import simpledb.file.BlockId;
    import simpledb.query.Constant;
    import simpledb.record.Layout;
    import simpledb.record.RID;
    import simpledb.tx.Transaction;

    /*
     * BTreeLeaf stores the contents of a B-Tree leaf block
    */
    public class BTreeLeaf {
      private Transaction tx;
      private Layout layout;
      private Constant searchkey;
      private BTPage contents;
      private int currentslot;
      private String filename;

      public BTreeLeaf(Transaction tx, BlockId blk, Layout layout, Constant searchkey) {
        this.tx = tx;
        this.layout = layout;
        this.searchkey = searchkey;
        contents = new BTPage(tx, blk, layout);
        filename = blk.fileName();
      }

      public void close() {
        contents.close();
      }

      public boolean next() {
        currentslot++;
        if (currentslot >= contents.getNumRecs())
          return tryOverflow();
        else if (contents.getDataVal(currentslot).equals(searchkey))
          return true;
        else
          return tryOverflow();
      }

      public RID getDataRid() {
        return contents.getDataRid(currentslot);
      }

      public void delete(RID datarid) {
        while (next())
          if (getDataRid().equals(datarid)) {
            contents.delete(currentslot);
            return;
          }
      }

      /*
       * Insert a new leaf record having the specified dataRID and previously-specified search key.
       */
      public DirEntry insert(RID datarid) {
        // If the new record does not fit in the page, split the page and return directory entry for the new page.
        if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchkey) > 0) {
          Constant firstval = contents.getDataVal(0);
          BlockId newblk = contents.split(0, contents.getFlag());
          currentslot = 0;
          contents.setFlag(-1);
          contents.insertLeaf(currentslot, searchkey, datarid);
          return new DirEntry(firstval, newblk.number());
        }

        currentslot++;
        contents.insertLeaf(currentslot, searchkey, datarid);
        if (!contents.isFull())
          return null;

        // if page is full, split it
        Constant firstkey = contents.getDataVal(0);
        Constant lastkey = contents.getDataVal(contents.getNumRecs() - 1);
        if (lastkey.equals(firstkey)) {
          // create an overflow block to hold all but the first record
          BlockId newblk = contents.split(1, contents.getFlag());
          contents.setFlag(newblk.number());
          return null;
        } else {
          int splitpos = contents.getNumRecs() / 2; // split into half
          Constant splitkey = contents.getDataVal(splitpos);
          if (splitkey.equals(firstkey)) {
            // move right, looking for the next key
            while (contents.getDataVal(splitpos).equals(splitkey))
              splitpos++;
            splitkey = contents.getDataVal(splitpos);
          } else {
            // move left, looking for first entry having that key
            while (contents.getDataVal(splitpos - 1).equals(splitkey))
              splitpos--;
          }
          BlockId newblk = contents.split(splitpos, -1);
          return new DirEntry(splitkey, newblk.number());
        }
      }

      private boolean tryOverflow() {
        Constant firstkey = contents.getDataVal(0);
        int flag = contents.getFlag();
        if (!searchkey.equals(firstkey) || flag < 0)
          return false;
        contents.close();
        BlockId nextblk = new BlockId(filename, flag);
        contents = new BTPage(tx, nextblk, layout);
        currentslot = 0;
        return true;
      }
    }
    ```
1. Create `BTreeDir`
    ```java
    package simpledb.index.btree;

    import simpledb.file.BlockId;
    import simpledb.query.Constant;
    import simpledb.record.Layout;
    import simpledb.tx.Transaction;

    /*
     * B-Tree directory block
     */
    public class BTreeDir {
      private Transaction tx;
      private Layout layout;
      private BTPage contents;
      private String filename;

      BTreeDir(Transaction tx, BlockId blk, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        contents = new BTPage(tx, blk, layout);
        filename = blk.fileName();
      }

      public void close() {
        contents.close();
      }

      /*
       * Move to the leaf node for the search key
       */
      public int search(Constant searchkey) {
        BlockId childblk = findChildBlock(searchkey);
        while (contents.getFlag() > 0) { // until reaching the leaf node
          contents.close();
          contents = new BTPage(tx, childblk, layout);
          childblk = findChildBlock(searchkey);
        }
        return childblk.number();
      }

      /*
       * Create a new root block.
       * The new block will have two children:
       * 1. the old root
       * 2. the specified block
       */
      public void makeNewRoot(DirEntry e) {
        Constant firstval = contents.getDataVal(0);
        int level = contents.getFlag();
        BlockId newblk = contents.split(0, level); // transfer all records
        DirEntry oldroot = new DirEntry(firstval, newblk.number());
        insertEntry(oldroot);
        insertEntry(e);
        contents.setFlag(level + 1);
      }

      /*
       * insert a new directory entry into the B-Tree block.
       * 1. If block is at level 0, the entry is just inserted there.
       * 2. Otherwise, the entry is inserted into the leaf node
       * Return the DirEntry if the block split
       */
      public DirEntry insert(DirEntry e) {
        if (contents.getFlag() == 0)
          return insertEntry(e);
        BlockId childblk = findChildBlock(e.dataVal());
        BTreeDir child = new BTreeDir(tx, childblk, layout);
        DirEntry myentry = child.insert(e);
        child.close();
        return (myentry != null) ? insertEntry(myentry) : null;
      }

      /*
       * Insert a new directory entry
       */
      private DirEntry insertEntry(DirEntry e) {
        int newslot = 1 + contents.findSlotBefore(e.dataVal());
        contents.insertDir(newslot, e.dataVal(), e.blockNumber());
        if (!contents.isFull())
          return null;
        int level = contents.getFlag();
        int splitpos = contents.getNumRecs() / 2;
        Constant splitval = contents.getDataVal(splitpos);
        BlockId newblk = contents.split(splitpos, level);
        return new DirEntry(splitval, newblk.number());
      }

      private BlockId findChildBlock(Constant searchkey) {
        int slot = contents.findSlotBefore(searchkey);
        if (contents.getDataVal(slot+1).equals(searchkey))
          slot++;
        int blknum = contents.getChildNum(slot);
        return new BlockId(filename, blknum);
      }
    }
    ```
1. Create `BTreeIndex`
    ```java
    package simpledb.index.btree;

    import static java.sql.Types.INTEGER;

    import simpledb.file.BlockId;
    import simpledb.index.Index;
    import simpledb.query.Constant;
    import simpledb.record.Layout;
    import simpledb.record.RID;
    import simpledb.record.Schema;
    import simpledb.tx.Transaction;

    public class BTreeIndex implements Index {
      private static final String DIR_FLD_BLOCK = "block";
      private static final String DIR_FLD_DATAVAL = "dataval";
      private Transaction tx;
      private Layout dirLayout;
      private Layout leafLayout;
      private String leaftbl;
      private BTreeLeaf leaf = null;
      private BlockId rootblk;

      public BTreeIndex(Transaction tx, String idxname, Layout leafLayout) {
        this.tx = tx;

        // deal with the leaves
        leaftbl = idxname + "leaf";
        this.leafLayout = leafLayout;
        if (tx.size(leaftbl) == 0) {
          BlockId blk = tx.append(leaftbl);
          BTPage node = new BTPage(tx, blk, leafLayout);
          node.format(blk, -1); // -1 means no overflow block
        }

        // deal with the directory
        Schema dirsch = new Schema();
        dirsch.add(DIR_FLD_BLOCK, leafLayout.schema());
        dirsch.add(DIR_FLD_DATAVAL, leafLayout.schema());
        String dirtbl = idxname + "dir";
        dirLayout = new Layout(dirsch);
        rootblk = new BlockId(dirtbl, 0);
        if (tx.size(dirtbl) == 0) {
          // create new root block
          tx.append(dirtbl);
          BTPage node = new BTPage(tx, rootblk, dirLayout);
          node.format(rootblk, 0);
          // insert initial directory entry
          int fldtype = dirsch.type(DIR_FLD_DATAVAL);
          Constant minval = (fldtype == INTEGER) ? new Constant(Integer.MIN_VALUE) : new Constant("");
          node.insertDir(0, minval, 0);
          node.close();
        }
      }

      /*
       * Travere the directory to find the leaf block corresponding to the specified
       * search key.
       * The leaf page is kept open, for use by the methods next and getDataRid.
       */
      @Override
      public void beforeFirst(Constant searchkey) {
        close();
        BTreeDir root = new BTreeDir(tx, rootblk, dirLayout);
        int blknum = root.search(searchkey);
        root.close();
        BlockId leafblk = new BlockId(leaftbl, blknum);
        leaf = new BTreeLeaf(tx, leafblk, leafLayout, searchkey);
      }

      @Override
      public boolean next() {
        return leaf.next();
      }

      @Override
      public RID getDataRid() {
        return leaf.getDataRid();
      }

      /*
       * insert the datarid to the position where the record with the dataval locates.
       * 1. if the leaf is not full, the process just finishes
       * 2. if the leaf splits, add the new node from the root.
       * 3. if the root splits, make a new root.
       */
      @Override
      public void insert(Constant dataval, RID datarid) {
        beforeFirst(dataval);
        DirEntry e = leaf.insert(datarid);
        leaf.close();
        if (e == null) // the leaf wasn't full
          return;

        // if leaf was full and split occurs
        BTreeDir root = new BTreeDir(tx, rootblk, dirLayout);
        DirEntry e2 = root.insert(e);
        if (e2 != null) // if root was split
          root.makeNewRoot(e2);
        root.close();
      }

      /*
       * 1. traverse to find the leaf page containing the record
       * 2. delete the record by datarid
       */
      @Override
      public void delete(Constant dataval, RID datarid) {
        beforeFirst(dataval);
        leaf.delete(datarid);
        leaf.close();
      }

      @Override
      public void close() {
        if (leaf != null)
          leaf.close();
      }

      /*
       * Estimate the number of block accesses
       * required to find all index records haivg a particular search key.
       */
      public static int searchCost(int numblocks, int rpb) {
        return 1 + (int) (Math.log(numblocks) / Math.log(rpb));
      }
    }
    ```

### 12.2. Plan & Planner

### 12.3. Test

### 12.4. Hash Index (Optional)