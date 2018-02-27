package simpledb.parallel;

import simpledb.*;
import simpledb.OpIterator;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some
 * partition function (provided as a PartitionFunction object during the
 * ShuffleProducer's instantiation).
 * 
 * */
public class ShuffleProducer extends Producer {

    private static final long serialVersionUID = 1L;

    public String getName() {
        return "shuffle_p";
    }

    public ShuffleProducer(OpIterator child, ParallelOperatorID operatorID,
                           SocketInfo[] workers, PartitionFunction<?, ?> pf) {
        super(operatorID);
        // some code goes here
    }

    public void setPartitionFunction(PartitionFunction<?, ?> pf) {
        // some code goes here
    }

    public SocketInfo[] getWorkers() {
        // some code goes here
        return null;
    }

    public PartitionFunction<?, ?> getPartitionFunction() {
        // some code goes here
        return null;
    }

    // some code goes here
    class WorkingThread extends Thread {
        public void run() {

            // some code goes here
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    }

    public void close() {
        // some code goes here
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return null;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }
}
