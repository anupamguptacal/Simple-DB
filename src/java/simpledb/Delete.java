package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId transactionId;
    private OpIterator child;
    private TupleDesc desc;
    boolean look;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.transactionId = t;
        this.child = child;
        Type[] typeArray = new Type[1];
        typeArray[0] = Type.INT_TYPE;
        String[] nameArray = new String[1];
        nameArray[0] = "Count";
        desc = new TupleDesc(typeArray, nameArray);
        look = false;
    }

    public TupleDesc getTupleDesc() {
        return this.desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.child.open();
        look = false;
    }

    public void close() {
        super.close();
        this.child.close();
        look = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        look = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(look) {
            return null;
        }
        look = true;
        int count = 0;
        while(child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().deleteTuple(this.transactionId, tuple);
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
        if(children[0] != this.child) {
            this.child = children[0];
        }
    }

}
