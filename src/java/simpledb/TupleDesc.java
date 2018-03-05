package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {


    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private List<TDItem> listOfFields;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return listOfFields.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        listOfFields = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            listOfFields.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        listOfFields = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i ++) {
            TDItem item = new TDItem(typeAr[i], null);
            listOfFields.add(item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return listOfFields.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i >= listOfFields.size() || i < 0) {
            throw new NoSuchElementException("No field found");
        }
        return listOfFields.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i >= listOfFields.size() || i < 0) {
            throw new NoSuchElementException();
        }
      return listOfFields.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i = 0; i < listOfFields.size(); i ++) {
            if(name == null && listOfFields.get(i).fieldName == null) {
                return i;
            } else if(listOfFields.get(i).fieldName != null && listOfFields.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(int i = 0; i < listOfFields.size(); i ++) {
            size += listOfFields.get(i).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] typeArray = new Type[td1.listOfFields.size() + td2.listOfFields.size()];
        String[] stringArray = new String[td1.getSize() + td2.getSize()];
        int index = 0;
        for(int i = 0; i < td1.listOfFields.size(); i++) {
            typeArray[index] = td1.listOfFields.get(i).fieldType;
            stringArray[index++] = td1.listOfFields.get(i).fieldName;
        }

        for(int i = 0; i < td2.listOfFields.size(); i ++) {
            typeArray[index] = td2.listOfFields.get(i).fieldType;
            stringArray[index++] = td2.listOfFields.get(i).fieldName;
        }
        return new TupleDesc(typeArray, stringArray);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if(o == this || (o == null && this == null)) {
            return true;
        } else if((o == null && this != null) || (o != null && this == null)) {
            return false;
        }
        if(!(o instanceof TupleDesc)) {
            return false;
        } else {
            TupleDesc tupleDesc = (TupleDesc) o;
            if(tupleDesc.listOfFields.size() != this.listOfFields.size()) {
                return false;
            }
            for(int i = 0; i < this.listOfFields.size(); i++) {
                if(!(this.listOfFields.get(i).fieldType.equals(tupleDesc.listOfFields.get(i).fieldType)) ) {
                    return false;
                }
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String result = "";
        for(int i = 0; i < listOfFields.size() - 1; i++) {
            result += listOfFields.get(i).fieldType + "(" + listOfFields.get(i).fieldName + ")";
        }
        result += listOfFields.get(listOfFields.size() - 1).fieldType + "(" + listOfFields.get(listOfFields.size() - 1).fieldName + ")";
        return result;
    }
}
