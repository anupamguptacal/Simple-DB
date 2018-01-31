package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private OpIterator child;
    private TransactionId transactionId;
    private int tableId;
    private static final long serialVersionUID = 1L;
    boolean called;
    TupleDesc desc;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tableId = tableId;
        this.child = child;
        this.transactionId = t;
        called = false;
        Type[] typeArray = new Type[1];
        typeArray[0] = Type.INT_TYPE;
        String[] nameArray = new String[1];
        nameArray[0] = "Count";
        desc = new TupleDesc(typeArray, nameArray);
    }

    public TupleDesc getTupleDesc() {
        return desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        called = false;
    }

    public void close() {
        super.close();
        child.close();
        called = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        called = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(called) {
            return null;
        }
        called = true;
        int count = 0;
        while(child.hasNext()) {
           Tuple tuple = child.next();
           try {
               Database.getBufferPool().insertTuple(this.transactionId, this.tableId, tuple);
               count++;
           } catch (IOException e) {
               throw new DbException(e.getMessage());
           }
        }
        Tuple returned = new Tuple(desc);
        returned.setField(0, new IntField(count));
        return returned;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if((children[0] != this.child)) {
            this.child = children[0];
        }
    }
}
