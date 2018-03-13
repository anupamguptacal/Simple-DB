package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile, Serializable {

    private File file;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
       return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        byte[] pageContent = new byte[pageSize];
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
            int offset = pid.getPageNumber() * pageSize;
            raf.seek(offset);
            raf.read(pageContent);
            HeapPageId id = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            Page page = new HeapPage(id, pageContent);
            raf.close();
            return page;
        } catch(Exception e) {
            System.out.println("An error occurred while reading the page from file " + e.getLocalizedMessage() + e.getMessage());
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
//        if(page.getId().getTableId() != this.getId()) {
//            throw new IOException("Page cannot be added to this table since it doesn't belong to it. TableID does not match");
//        }
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        raf.seek(offset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (Math.ceil((file.length()/BufferPool.getPageSize())));
    }

    // see DbFile.java for javadocs
    // Still to implement : Creating new Pages
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        int pageNumber = 0;
        boolean found = false;
        ArrayList<Page> storage = new ArrayList<Page>();
        try {
            while (!found) {
                synchronized (this) {
                    if (pageNumber * BufferPool.getPageSize() >= this.file.length()) {
                        try {
                            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
                            raf.seek(pageNumber * BufferPool.getPageSize());
                            raf.write(HeapPage.createEmptyPageData());
                            raf.close();
                        } catch (Exception e) {
                            throw new IOException(e.getMessage());
                        }
                    }
                }
                HeapPageId heapPageId = new HeapPageId(getId(), pageNumber);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
                if (heapPage.getNumEmptySlots() == 0) {
                    pageNumber++;
                    Database.getBufferPool().releasePage(tid, heapPageId);
                    continue;
                } else {
                    heapPage.insertTuple(t);
                    heapPage.markDirty(true, tid);
                }
                storage.add(heapPage);
                found = true;
            }

            return storage;
        } catch (DbException e) {
            throw new DbException (e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId recordId = t.getRecordId();
       if(!(recordId.getPageId().getTableId() == (this.getId()))) {
           throw new DbException("Tuple belongs to different Table");
       } else {
           try {
               ArrayList<Page> storage = new ArrayList<Page>();
               HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
               page.deleteTuple(t);
               page.markDirty(true, tid);
               storage.add(page);
               return storage;
           } catch(DbException e) {
               throw new DbException(e.getMessage() + e.getLocalizedMessage());
           }
       }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);/*DbFileIterator() {
            HeapPage page;
            Iterator<Tuple> tupleIterator;
            int pageNumber;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageNumber = 0;
                HeapPageId heapPageId = new HeapPageId(getId(), pageNumber);
                page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                tupleIterator = page.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(page == null || tupleIterator == null) {
                    return false;
                }
                if(tupleIterator.hasNext()) {
                    return true;
                } else {
                    while(pageNumber < numPages() - 1) {
                        pageNumber ++;
                        HeapPageId heapPageId = new HeapPageId(getId(), pageNumber);
                        page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                        tupleIterator = page.iterator();
                        if(tupleIterator.hasNext()) {
                            return true;
                        }
                    }
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(hasNext()) {
                  return tupleIterator.next();
                } else {
                    throw new NoSuchElementException("No elements found to return");
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageNumber = 0;
                HeapPageId heapPageId = new HeapPageId(getId(), pageNumber);
                page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                tupleIterator = page.iterator();
            }

            @Override
            public void close() {
                page = null;
                tupleIterator = null;
                pageNumber = 0;
            }
        };*/
    }

    private class HeapFileIterator implements DbFileIterator, Serializable {
        transient HeapPage page;
        transient Iterator<Tuple> tupleIterator;
        int pageNumber;
        TransactionId tid;

        HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNumber = 0;
            HeapPageId heapPageId = new HeapPageId(getId(), pageNumber);
            page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
            tupleIterator = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(page == null || tupleIterator == null) {
                return false;
            }
            if(tupleIterator.hasNext()) {
                return true;
            } else {
                while(pageNumber < numPages() - 1) {
                    pageNumber ++;
                    HeapPageId heapPageId = new HeapPageId(getId(), pageNumber);
                    page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                    tupleIterator = page.iterator();
                    if(tupleIterator.hasNext()) {
                        return true;
                    }
                }
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(hasNext()) {
                return tupleIterator.next();
            } else {
                throw new NoSuchElementException("No elements found to return");
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageNumber = 0;
            HeapPageId heapPageId = new HeapPageId(getId(), pageNumber);
            page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
            tupleIterator = page.iterator();
        }

        @Override
        public void close() {
            page = null;
            tupleIterator = null;
            pageNumber = 0;
        }
    }

}

