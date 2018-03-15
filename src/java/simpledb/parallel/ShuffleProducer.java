package simpledb.parallel;

import org.apache.mina.core.future.IoFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.IoSession;
import simpledb.*;
import simpledb.OpIterator;

import java.util.ArrayList;

/**
 * The producer part of the Shuffle Exchange operator.
 *
 * ShuffleProducer distributes tuples to the workers according to some
 * partition function (provided as a PartitionFunction object during the
 * ShuffleProducer's instantiation).
 * Anupam Gupta
 * */
public class ShuffleProducer extends Producer {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private ParallelOperatorID operatorID;
    private SocketInfo[] workers;
    private PartitionFunction<?, ?> pf;
    private WorkingThread runningThread;

    public String getName() {
        return "shuffle_p";
    }

    public ShuffleProducer(OpIterator child, ParallelOperatorID operatorID,
                           SocketInfo[] workers, PartitionFunction<?, ?> pf) {
        super(operatorID);
        this.child = child;
        this.operatorID = operatorID;
        this.workers = workers;
        this.pf = pf;
    }

    public void setPartitionFunction(PartitionFunction<?, ?> pf) {
        this.pf = pf;
    }

    public SocketInfo[] getWorkers() {
        return this.workers;
    }

    public PartitionFunction<?, ?> getPartitionFunction() {
        return this.pf;
    }

    // some code goes here
    class WorkingThread extends Thread {
        IoSession[] sessions = new IoSession[ShuffleProducer.this.workers.length];
        public void run() {
            for(int i = 0; i < ShuffleProducer.this.workers.length; i++) {
                IoSession session = ParallelUtility.createSession(
                        ShuffleProducer.this.workers[i].getAddress(),
                        ShuffleProducer.this.getThisWorker().minaHandler, -1);
                sessions[i] = session;
            }
            try {
                ArrayList<Tuple>[] bufferStorage = (ArrayList<Tuple>[]) new ArrayList[ShuffleProducer.this.workers.length];
                for(int i = 0; i < bufferStorage.length; i++) {
                    bufferStorage[i] = new ArrayList<Tuple>();
                }
                long lastTime = System.currentTimeMillis();
                while (ShuffleProducer.this.child.hasNext()) {
                    Tuple tuple = ShuffleProducer.this.child.next();
                    int partitionValue = pf.partition(tuple, getTupleDesc());
                    bufferStorage[partitionValue].add(tuple);
                    int cnt = bufferStorage[partitionValue].size();
                    if (cnt >= TupleBag.MAX_SIZE) {
                        IoSession session = sessions[partitionValue];
                        session.write(new TupleBag(
                                ShuffleProducer.this.operatorID,
                                ShuffleProducer.this.getThisWorker().workerID,
                                bufferStorage[partitionValue].toArray(new Tuple[]{}),
                                ShuffleProducer.this.getTupleDesc()
                        ));
                        bufferStorage[partitionValue].clear();
                        lastTime = System.currentTimeMillis();
                    }
                    if (cnt >= TupleBag.MIN_SIZE) {
                        long thisTime = System.currentTimeMillis();
                        if (thisTime - lastTime > TupleBag.MAX_MS) {
                            IoSession session = sessions[partitionValue];
                            session.write(new TupleBag(
                                    ShuffleProducer.this.operatorID,
                                    ShuffleProducer.this.getThisWorker().workerID,
                                    bufferStorage[partitionValue].toArray(new Tuple[]{}),
                                    ShuffleProducer.this.getTupleDesc()
                            ));
                            bufferStorage[partitionValue].clear();
                            lastTime = thisTime;
                        }
                    }
                }
                IoFutureListener<WriteFuture> something = new IoFutureListener<WriteFuture>() {
                    @Override
                    public void operationComplete(WriteFuture ioFuture) {
                        ParallelUtility.closeSession(ioFuture.getSession());
                    }
                };
                for(int i = 0; i < bufferStorage.length; i++) {
                    if(bufferStorage[i].size() > 0) {
                        sessions[i].write(new TupleBag(
                                ShuffleProducer.this.operatorID,
                                ShuffleProducer.this.getThisWorker().workerID,
                                bufferStorage[i].toArray(new Tuple[]{}),
                                ShuffleProducer.this.getTupleDesc()
                        ));
                    }
                    sessions[i].write(new TupleBag(ShuffleProducer.this.operatorID, ShuffleProducer.this.getThisWorker().workerID)).addListener(/*new IoFutureListener<IoFuture>() {
                        @Override
                        public void operationComplete(IoFuture ioFuture) {
                            ParallelUtility.closeSession(ioFuture.getSession());
                        }
                    }*/ something);
                }
            } catch (DbException e) {
                e.printStackTrace();
                System.out.println("DbException thrown by Shuffle Producer " + e.getLocalizedMessage());
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
                System.out.println("TransactionAbortedException thrown by Shuffle Producer = " + e.getLocalizedMessage());
            }
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        this.runningThread = new WorkingThread();
        this.runningThread.start();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        try {
            // wait until the working thread terminate and return an empty tuple set
            runningThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if(this.child != children[0]) {
            this.child = children[0];
        }
    }
}
