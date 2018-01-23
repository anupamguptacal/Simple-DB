package simpledb;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;
    Map<Field, Integer> storage;
    Map<Field, Integer> countStorer;
    int nonGroupStore;
    boolean initialized;
    int totalNumber;
    boolean avgCalculated;
    boolean returnedValue;
    String aggregateColumn;
    Map<Field, Integer> averageStorer;
    private static final long serialVersionUID = 1L;
    Iterator<Field> iterator;
    TupleDesc desc;
    Tuple tuple;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        storage = new HashMap<Field, Integer>();
        countStorer = new HashMap<Field, Integer>();
        initialized = false;
        nonGroupStore = 0;
        totalNumber = 0;
        avgCalculated = false;
        returnedValue = false;
        iterator = null;
        averageStorer = new HashMap<Field, Integer>();
        if(this.gbfield != NO_GROUPING) {
            Type[] array = new Type[2];
            array[0] = gbfieldType;
            array[1] = Type.INT_TYPE;
            String[] nameArray = new String[2];
            nameArray[0] = "groupValue";
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        aggregateColumn = tup.getTupleDesc().getFieldName(this.afield);
        if(this.gbfield != NO_GROUPING) {
            Field value = tup.getField(this.gbfield);
            Field aggregate = tup.getField(this.afield);
            int aggregate_value = Integer.valueOf(aggregate.toString());
            if(!storage.containsKey(value)) {
                if(what == Op.COUNT) {
                    storage.put(value, 1);
                } else if(what == Op.AVG) {
                    storage.put(value, aggregate_value);
                    countStorer.put(value, 1);
                }
                else {
                    storage.put(value, aggregate_value);
                }
            } else {
                Integer stored_val = storage.get(value);
                if(what == Op.MIN) {
                    if(stored_val > aggregate_value) {
                        storage.put(value, aggregate_value);
                    }
                } else if( what == Op.MAX) {
                    if(stored_val < aggregate_value) {
                        storage.put(value, aggregate_value);
                    }
                } else if(what == Op.SUM) {
                    storage.put(value, stored_val + aggregate_value);
                } else if(what == Op.COUNT) {
                    storage.put(value, storage.get(value) + 1);
                } else if(what == Op.AVG) {
                    storage.put(value, stored_val + aggregate_value);
                    countStorer.put(value, countStorer.get(value) + 1);
                }
            }
        } else {
            Field aggregate = tup.getField(this.afield);
            int aggregate_value = Integer.valueOf(aggregate.toString());
            if(!initialized) {
                if(what == Op.COUNT) {
                    nonGroupStore = 1;
                } else {
                    nonGroupStore = aggregate_value;
                }
                totalNumber = 1;
                initialized = true;
            } else {
                if(what == Op.MIN) {
                    if(aggregate_value < nonGroupStore) {
                        nonGroupStore = aggregate_value;
                    }
                } else if(what == Op.MAX) {
                    if(aggregate_value > nonGroupStore) {
                        nonGroupStore = aggregate_value;
                    }
                } else if(what == Op.SUM) {
                    nonGroupStore += aggregate_value;
                } else if(what == Op.COUNT) {
                    nonGroupStore += 1;
                } else if(what == Op.AVG) {
                    nonGroupStore += aggregate_value;
                    totalNumber += 1;
                }
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            boolean openFlag = false;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                openFlag = true;
                returnedValue = false;
                if (what == Op.AVG) {
                    if (gbfield != NO_GROUPING) {
                        for (Map.Entry<Field, Integer> entry : storage.entrySet()) {
                            int numberOfOccurences = countStorer.get(entry.getKey());
                            int sum = entry.getValue();
                            averageStorer.put(entry.getKey(), sum / numberOfOccurences);
                        }
                    } else {
                        nonGroupStore = nonGroupStore / totalNumber;
                    }
                }
                if(gbfield != NO_GROUPING) {
                    if(what == Op.AVG) {
                        iterator = averageStorer.keySet().iterator();
                    } else {
                        iterator = storage.keySet().iterator();
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!openFlag) {
                    return false;
                }
                if(gbfield == NO_GROUPING) {
                    return !returnedValue;
                } else {
                    return iterator.hasNext();
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(hasNext()) {
                    if (gbfield == NO_GROUPING) {
                        tuple.setField(0, new IntField(nonGroupStore));
                        returnedValue = true;
                    } else {
                        Field key = iterator.next();
                        int value;
                        if(what == Op.AVG) {
                            value = averageStorer.get(key);
                        } else {
                            value = storage.get(key);
                        }
                        tuple.setField(0, key);
                        tuple.setField(1, new IntField(value));
                    }
                    return tuple;
                } else {
                    throw new NoSuchElementException("No more elements to return");
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                returnedValue = false;
                if(gbfield != NO_GROUPING) {
                    iterator = storage.keySet().iterator();
                }
                tuple = new Tuple(desc);
            }

            @Override
            public TupleDesc getTupleDesc() {
                return desc;
            }

            @Override
            public void close() {
                iterator = null;
                returnedValue = false;
                openFlag = false;
            }
        };
    }
}
