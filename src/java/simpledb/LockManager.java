package simpledb;
import java.util.*;
import java.util.Map.Entry;

//Anupam Gupta
public class LockManager {

    Map<PageId, TransactionId> exclusiveLock;
    Map<PageId, Set<TransactionId>> readLock;
    Map<TransactionId, Set<TransactionId>> dependencyMapping;

    public LockManager() {
        exclusiveLock = new HashMap<PageId, TransactionId>();
        readLock = new HashMap<PageId, Set<TransactionId>>();
        dependencyMapping = new HashMap<TransactionId, Set<TransactionId>>();
    }

    public synchronized void releaseTransaction(TransactionId tid) {
        Set<PageId> toRelease = new HashSet<PageId>();
        for(PageId pageId : exclusiveLock.keySet()) {
            if(exclusiveLock.get(pageId).equals(tid)) {
                toRelease.add(pageId);
            }
        }
        for(PageId pageId: toRelease) {
            exclusiveLock.remove(pageId);
        }
        for(PageId pageId: readLock.keySet()) {
            Set<TransactionId> storeValues = readLock.get(pageId);
            storeValues.remove(tid);
            readLock.put(pageId, storeValues);
        }
    }

    public synchronized boolean getReadLock(PageId pageId, TransactionId transactionId) throws TransactionAbortedException {
        if(exclusiveLock.containsKey(pageId)) {
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
                if(!addToDependencyMapping(exclusiveLock.get(pageId), transactionId)) {
                    throw new TransactionAbortedException();
                }
                return false;
            }
        } else {
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
        }
    }

    public synchronized void removeFromDependency(TransactionId transactionId) {
        dependencyMapping.remove(transactionId);
        for(Entry<TransactionId, Set<TransactionId>> entry: dependencyMapping.entrySet()) {
            Set<TransactionId> valueStored = entry.getValue();
            valueStored.remove(transactionId);
            dependencyMapping.put(entry.getKey(), valueStored);
        }
    }

    public synchronized boolean releaseReadLock(PageId pageId, TransactionId transactionId) {
        if(readLock.containsKey(pageId)) {
            Set<TransactionId> storeValues = readLock.get(pageId);
            storeValues.remove(transactionId);

            readLock.put(pageId, storeValues);
        }
        return true;
    }

    public synchronized boolean identifyDeadLock(TransactionId transactionId, PageId pageId) {
        if(exclusiveLock.containsKey(pageId) && !exclusiveLock.get(pageId).equals(transactionId)) {
            // some other transaction has an exclusive lock on the page I want.
            // deadlock if I have an exclusive lock on some page the same transaction wants
            if(dependencyMapping.containsKey(transactionId)) {
                Set<TransactionId> transactionsWaitingOnMe = dependencyMapping.get(transactionId);
                if(transactionsWaitingOnMe.contains(exclusiveLock.get(pageId))) {
                    return true;
                }
            }
            TransactionId holdingLock = exclusiveLock.get(pageId);
            Set<TransactionId> transactionSet = new HashSet<TransactionId>();
            if(dependencyMapping.containsKey(holdingLock)) {
                transactionSet = dependencyMapping.get(holdingLock);
                transactionSet.add(transactionId);
            }
            dependencyMapping.put(holdingLock, transactionSet);
        }
        return false;
    }

    public synchronized boolean addToDependencyMapping(TransactionId toAddTo, TransactionId transactionId) {
        if(identifyDeadLockExists(transactionId, toAddTo)) {
            return false;
        }
        if(!dependencyMapping.containsKey(toAddTo)) {
            dependencyMapping.put(toAddTo, new HashSet<TransactionId>());
        }
        Set<TransactionId> transactionIds = dependencyMapping.get(toAddTo);
        transactionIds.add(transactionId);
        dependencyMapping.put(toAddTo, transactionIds);
        return true;
    }

    public synchronized boolean identifyDeadLockExists(TransactionId first, TransactionId second) {
        if(dependencyMapping.containsKey(first)) {
            Set<TransactionId> transactionsWaitingOnMe = dependencyMapping.get(first);
            if(transactionsWaitingOnMe.contains(second)) {
                return true;
            }
        }
        return false;
    }

    public synchronized boolean getExclusiveLock(PageId pageId, TransactionId transactionId) throws TransactionAbortedException{
        if(exclusiveLock.containsKey(pageId) && (!exclusiveLock.get(pageId).equals(transactionId))) {
            if(!addToDependencyMapping(exclusiveLock.get(pageId), transactionId)) {
                throw new TransactionAbortedException();
            }
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
                        if(identifyDeadLock(transactionId, pageId)) {
                            throw new TransactionAbortedException();
                        }

                        exclusiveLock.put(pageId, transactionId);
                        return true;
                    } else {
                        if(!addToDependencyMapping(readLock.get(pageId).iterator().next(), transactionId)) {
                            throw new TransactionAbortedException();
                        }
                        return false;
                    }
                } else {
                    Iterator<TransactionId> iterator = readLock.get(pageId).iterator();
                    while(iterator.hasNext()) {
                        if(!addToDependencyMapping(iterator.next(), transactionId)) {
                            throw new TransactionAbortedException();
                        }
                    }
                    return false;
                }
            } else {
                if(identifyDeadLock(transactionId, pageId)) {
                    throw new TransactionAbortedException();
                }

                exclusiveLock.put(pageId, transactionId);
                return true;
            }
        } else {
            if(identifyDeadLock(transactionId, pageId)) {
                throw new TransactionAbortedException();
            }

            exclusiveLock.put(pageId, transactionId);
            return true;
        }
    }

    public synchronized boolean releaseExclusiveLock(PageId pageId, TransactionId transactionId) {
        if(exclusiveLock.containsKey(pageId)) {
            if(exclusiveLock.get(pageId).equals(transactionId)) {

                exclusiveLock.remove(pageId);
                return true;

            } else {
                return false;
            }
        } else {
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
