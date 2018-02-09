package simpledb;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

public class LockManager {

     Map<PageId, TransactionId> exclusiveLock; //= new HashMap<PageId, TransactionId>();
     Map<PageId, Set<TransactionId>> readLock;//= new HashMap<PageId, Set<TransactionId>>();

    public LockManager() {
        exclusiveLock = new HashMap<PageId, TransactionId>();
        readLock = new HashMap<PageId, Set<TransactionId>>();
    }

    public synchronized boolean getReadLock(PageId pageId, TransactionId transactionId) {
        //System.out.println("Came to getReadLock, transaction = " + transactionId);
        if(exclusiveLock.containsKey(pageId)) {
            //System.out.println("Has Exclusive lock also");
            // If exclusive lock is on the same page by the same transaction then it can have read lock also
            if(transactionId.equals(exclusiveLock.get(pageId))) {
                if(readLock.containsKey(pageId)) {
                    Set<TransactionId> valueStored = readLock.get(pageId);
                    valueStored.add(transactionId);
                    readLock.put(pageId, valueStored);

                } else {
                    HashSet<TransactionId> insertion = new HashSet<TransactionId>();
                    insertion.add(transactionId);
                    readLock.put(pageId, insertion);
                }
                return true;
            } else {
                return false;
            }
        } else {
            //System.out.println("Does not have exclusive lock");
            // If not held by exclusive lock just add to readLock
            if(readLock.containsKey(pageId)) {
                Set<TransactionId> valueStored = readLock.get(pageId);
                valueStored.add(transactionId);
                readLock.put(pageId, valueStored);

            } else {
                HashSet<TransactionId> insertion = new HashSet<TransactionId>();
                insertion.add(transactionId);
                readLock.put(pageId, insertion);
            }
            //System.out.println("Returning true" + readLock.keySet());
            return true;
        }
    }

    public synchronized boolean releaseReadLock(PageId pageId, TransactionId transactionId) {
        //System.out.println("Release Read Lock called");
        if(readLock.containsKey(pageId)) {
            Set<TransactionId> storeValues = readLock.get(pageId);
            storeValues.remove(transactionId);
            readLock.put(pageId, storeValues);
        }
        return true;
    }

    public synchronized boolean getExclusiveLock(PageId pageId, TransactionId transactionId) {
        // If there is a read lock on the page by the same transaction only, then it's okay to get the exclusive lock
        if(exclusiveLock.containsKey(pageId)) {
            return false;
        }

        //If read lock exists do further checking otherwise just allow the exclusive lock since it's not read locked
        if(readLock.containsKey(pageId)) {
            //If read lock holds empty set or the only read lock is by the same transaction then it's okay allow
            // the exclusive lock
            Set<TransactionId> returnValue = readLock.get(pageId);
            if(returnValue.size() != 0) {
                if(returnValue.size() == 1) {
                    if(returnValue.contains(transactionId)) {
                        exclusiveLock.put(pageId, transactionId);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                exclusiveLock.put(pageId, transactionId);
                return true;
            }
        } else {
            exclusiveLock.put(pageId, transactionId);
            return true;
        }
    }

    public synchronized boolean releaseExclusiveLock(PageId pageId, TransactionId transactionId) {
        //System.out.println("Release Exclusive Lock called");
        //If not released by the same transaction don't remove exclusive lock else remove.
        if(exclusiveLock.containsKey(pageId)) {
            if(exclusiveLock.get(pageId).equals(transactionId)) {
                exclusiveLock.remove(pageId);
                //System.out.println("Exclusive lock removed") ;
                return true;
            } else {
                return false;
            }
        } else {
            //System.out.println("No exclusive lock found for this page");
            return true;
        }
    }

    public synchronized boolean hasReadLock(PageId pageId, TransactionId tid) {
        return readLock.containsKey(pageId) && readLock.get(pageId).contains(tid);
    }

    public synchronized boolean hasExclusiveLock(PageId pageId, TransactionId transactionId) {
        return exclusiveLock.containsKey(pageId) && exclusiveLock.get(pageId).equals(transactionId);
    }
}
