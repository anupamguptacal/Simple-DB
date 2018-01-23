package simpledb;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    String aggregateColumn;
    Map<Field, Integer> storage;
    int count;
    TupleDesc desc;
    Tuple tuple;
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        count = 0;
        storage = new HashMap<Field, Integer>();
        if(what != Op.COUNT) {
            throw new IllegalArgumentException("Only supports Count");
        }
        if(gbfield != NO_GROUPING) {
            Type[] array = new Type[2];
            array[0] = gbfieldtype;
            array[1] = Type.INT_TYPE;
            String[] nameArray = new String[2];
            nameArray[0] = "Group Value";
            nameArray[1] = what.toString() + "(" + aggregateColumn + ")";
            desc = new TupleDesc(array, nameArray);
            tuple = new Tuple(desc);
        } else {
            Type[] array = new Type[1];
            array[0] = Type.INT_TYPE;
            String[] nameArray = new String[1];
            nameArray[0] = what.toString() + "(" + aggregateColumn + ")";
            desc = new TupleDesc(array, nameArray);
            tuple = new Tuple(desc);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        aggregateColumn = tup.getTupleDesc().getFieldName(this.afield);
        if(gbfield != NO_GROUPING) {
            Field groupBy = tup.getField(this.gbfield);
            if(!storage.containsKey(groupBy)) {
                storage.put(groupBy, 1);
            } else {
                storage.put(groupBy, storage.get(groupBy) + 1);
            }
        } else {
            count ++;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            Boolean returned;
            Iterator<Field> iterator;
            Boolean openFlag = false;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                openFlag = true;
                returned = false;
                if(gbfield != NO_GROUPING) {
                    iterator = storage.keySet().iterator();
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!openFlag) {
                    return false;
                }
                if(gbfield != NO_GROUPING) {
                    return !returned;
                } else {
                    return iterator.hasNext();
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(hasNext()) {
                    if (gbfield != NO_GROUPING) {
                        Field key = iterator.next();
                        int value = storage.get(key);
                        tuple.setField(0, key);
                        tuple.setField(1, new IntField(value));
                    } else {
                        tuple.setField(0, new IntField(count));
                        returned = true;
                    }
                    return tuple;
                } else {
                    throw new NoSuchElementException("No more values to return");
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                returned = false;
                iterator = storage.keySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return desc;
            }

            @Override
            public void close() {
                returned = false;
                iterator = null;
                desc = null;
                tuple = null;
                openFlag = false;
            }
        };
    }

}
