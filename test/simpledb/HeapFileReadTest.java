package simpledb;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class HeapFileReadTest extends SimpleDbTestBase {
    private HeapFile hf;
    private TransactionId tid;
    private TupleDesc td;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        hf = SystemTestUtil.createRandomHeapFile(2, 20, null, null);
        td = Utility.getTupleDesc(2);
        tid = new TransactionId();
    }

    @After
    public void tearDown() throws Exception {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for HeapFile.getId()
     */
    @Test
    public void getId() throws Exception {
        int id = hf.getId();

        // NOTE(ghuo): the value could be anything. test determinism, at least.
        assertEquals(id, hf.getId());
        assertEquals(id, hf.getId());

        HeapFile other = SystemTestUtil.createRandomHeapFile(1, 1, null, null);
        assertTrue(id != other.getId());
    }

    /**
     * Unit test for HeapFile.getTupleDesc()
     */
    @Test
    public void getTupleDesc() throws Exception {    	
        assertEquals(td, hf.getTupleDesc());        
    }
    /**
     * Unit test for HeapFile.numPages()
     */
    @Test
    public void numPages() throws Exception {
        assertEquals(1, hf.numPages());
        // assertEquals(1, empty.numPages());
    }

    /**
     * Unit test for HeapFile.readPage()
     */
    @Test
    public void readPage() throws Exception {
        HeapPageId pid = new HeapPageId(hf.getId(), 0);
        HeapPage page = (HeapPage) hf.readPage(pid);

        // NOTE(ghuo): we try not to dig too deeply into the Page API here; we
        // rely on HeapPageTest for that. perform some basic checks.
        assertEquals(484, page.getNumEmptySlots());
        assertTrue(page.isSlotUsed(1));
        assertFalse(page.isSlotUsed(20));
    }

    @Test
    public void testIteratorBasic() throws Exception {
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 3, null,
                null);

        DbFileIterator it = smallFile.iterator(tid);
        // Not open yet
        assertFalse(it.hasNext());
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }

        it.open();
        int count = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            count += 1;
        }
        assertEquals(3, count);
        it.close();
    }

    @Test
    public void testIteratorEmptyPage() throws Exception {
        // Create HeapFile/Table
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 3, null,
                null);
        int tdSize = 8;
        int numTuples = (BufferPool.getPageSize()*8) / (tdSize * 8 + 1);
        int headerSize = (int) Math.ceil(numTuples / 8.0);
        // Leave these as all zeroes so this entire page is empty
        byte[] empty = new byte[numTuples * 8 + headerSize];
        byte[] full = new byte[numTuples * 8 + headerSize];
        // Since every bit is marked as used, every tuple should be used,
        // and all should be set to -1.
        for (int i = 0; i < full.length; i++) {
            full[i] = 0xFFFFFFFF;
        }
        // Grab valid table ids (these are randomly generated)
        Iterator<Integer> tableIdIt = Database.getCatalog().tableIdIterator();
        int tableId = tableIdIt.next();
        // The first two pages and the fourth page are empty and should be skipped
        // while still continuing on to read the third and fifth page.
        // Hint: You can see this in HeapFile's iterator right after assigning the
        // next HeapPage's iterator and checking if it's empty (hasNext()), making
        // sure it moves onto the next page until hitting the final page.
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 0), empty));
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 1), empty));
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 2), full));
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 3), empty));
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 4), full));
        DbFileIterator it = smallFile.iterator(tid);
        it.open();
        int count = 0;
        while (it.hasNext()) {
            Tuple t = it.next();
            assertNotNull(t);
            count += 1;
        }
        // Since we have two full pages, we should see all of 2*numTuples.
        assertEquals(2*numTuples, count);
        it.close();
    }

    @Test
    public void testIteratorClose() throws Exception {
        // make more than 1 page. Previous closed iterator would start fetching
        // from page 1.
        HeapFile twoPageFile = SystemTestUtil.createRandomHeapFile(2, 520,
                null, null);

        DbFileIterator it = twoPageFile.iterator(tid);
        it.open();
        assertTrue(it.hasNext());
        it.close();
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }
        // close twice is harmless
        it.close();
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileReadTest.class);
    }
}
