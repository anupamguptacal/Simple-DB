package simpledb;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * Anupam Gupta
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private ConcurrentHashMap<PageId, Page> pageMap;
    int maxPages;
    LockManager lockManager;


    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pageMap = new ConcurrentHashMap<PageId, Page>();
        maxPages = numPages;
        lockManager = new LockManager();

    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    public LockManager getLockManager() {
        return this.lockManager;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        //synchronized (pageMap) {
        boolean gotLock = false;
        while (!gotLock) {
            synchronized (this) {
                if (perm == Permissions.READ_ONLY) {
                    gotLock = this.lockManager.getReadLock(pid, tid);
                } else {
                    gotLock = this.lockManager.getExclusiveLock(pid, tid);
                }
            }
            if (!gotLock) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        if (pageMap.containsKey(pid)) {
            return pageMap.get(pid);
        }
        evictPage();
        int tableId = pid.getTableId();
        Page returned = Database.getCatalog().getDatabaseFile(tableId).readPage(pid);
        pageMap.put(pid, returned);
        return returned;
        //}
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        this.lockManager.releaseExclusiveLock(pid, tid);
        this.lockManager.releaseReadLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return this.lockManager.hasExclusiveLock(p, tid) || this.lockManager.hasReadLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        if (commit) {
            for (Entry<PageId, Page> entry : pageMap.entrySet()) {
                if (holdsLock(tid, entry.getKey())) {
                    Database.getLogFile().logWrite(tid, entry.getValue().getBeforeImage(), entry.getValue());
                    Database.getLogFile().force();
                    entry.getValue().setBeforeImage();
                }
            }
        } else {
            for (Entry<PageId, Page> entry : pageMap.entrySet()) {
                if (entry.getValue().isDirty() != null && entry.getValue().isDirty().equals(tid)) {
                    Page returned = Database.getCatalog().getDatabaseFile(entry.getKey().getTableId()).readPage(entry.getKey());
                    returned.markDirty(false, null);
                    pageMap.put(entry.getKey(), returned);
                }
            }
        }
        this.lockManager.releaseTransaction(tid);
        this.lockManager.removeFromDependency(tid);
    }


    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> changed = file.insertTuple(tid, t);
        for (Page page : changed) {
            page.markDirty(true, tid);
            if (pageMap.containsKey(page.getId())) {
                pageMap.put(page.getId(), page);
            } else {
                evictPage();
                pageMap.put(page.getId(), page);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> changed = file.deleteTuple(tid, t);
        for (Page page : changed) {
            page.markDirty(true, tid);
            evictPage();
            pageMap.put(page.getId(), page);
        }
    }


    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pageId : pageMap.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public void discardPage(PageId pid) {
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (pageMap.containsKey(pid)) {
            Page page = pageMap.get(pid);
            if (page.isDirty() != null) {
                HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
                TransactionId dirtier = page.isDirty();
                if (dirtier != null) {
                    Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                    Database.getLogFile().force();
                }
                file.writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        synchronized (pageMap) {
            if (pageMap.keySet().size() == maxPages) {

                int randomNumber = (int) (Math.random() * (pageMap.size() - 1));
                ArrayList<PageId> arrayList = new ArrayList<PageId>(pageMap.keySet());
                PageId pageId = arrayList.get(randomNumber);
                try {
                    flushPage(pageId);
                } catch (IOException e) {
                    throw new DbException("Page could not be flushed while evicting");
                }
                pageMap.remove(pageId);
            }
        }
    }

}
