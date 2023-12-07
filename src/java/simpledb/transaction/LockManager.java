package simpledb.transaction;

import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

public class LockManager {
    static private class TableIndex {
        public TransactionId tid;
        public PageId pid;

        TableIndex(TransactionId tid, PageId pid) {
            this.tid = tid;
            this.pid = pid;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TableIndex) {
                return this.tid.equals(((TableIndex) o).tid)
                        && this.pid.equals(((TableIndex) o).pid);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return tid.hashCode() * 7 + pid.hashCode();
        }
    }

    static private class LockTuple {
        public TransactionId tid;
        public PageId pid;
        public Long stamp;
        public Permissions perm;

        LockTuple(TransactionId tid, PageId pid, Long stamp, Permissions perm) {
            this.tid = tid;
            this.pid = pid;
            this.stamp = stamp;
            this.perm = perm;
        }
    }

    static private class ReferenceCounter {
        private final HashMap<PageId, Long> counter;

        ReferenceCounter() {
            counter = new HashMap<>();
        }

        public void add(PageId pid) {
            counter.putIfAbsent(pid, 0L);
            counter.put(pid, counter.get(pid) + 1);
        }

        public void delete(PageId pid) {
            counter.put(pid, counter.get(pid) - 1);
        }

        public boolean isEmpty(PageId pid) {
            return counter.get(pid) == 0;
        }
    };

    private final HashMap<TableIndex, LockTuple> lockTable;

    private final HashMap<PageId, StampedLock> lockRegister;

    private final ReferenceCounter counter;


    public LockManager() {
        this.lockTable = new HashMap<>();
        this.lockRegister = new HashMap<>();
        counter = new ReferenceCounter();
    }

    public void acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
        synchronized (this) {
            registerLock(pid);
            switch (perm) {
                case READ_WRITE:
                    acquireWriteLock(tid, pid);
                    break;
                case READ_ONLY:
                    acquireReadLock(tid, pid);
                    break;
                default:
                    throw new RuntimeException("cannot drop to here");
            }
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        synchronized (this) {
            StampedLock lock = lockRegister.get(pid);
            TableIndex index = new TableIndex(tid, pid);
            Long stamp = lockTable.get(index).stamp;
            lock.unlock(stamp);
            lockTable.remove(index);
            counter.delete(pid);
            if (counter.isEmpty(pid))
                lockRegister.remove(pid);
        }
    }

    public ArrayList<PageId> releaseAllLock(TransactionId tid) {
        synchronized (this) {
            ArrayList<PageId> list = new ArrayList<>();
            for (TableIndex index : lockTable.keySet()) {
                if (index.tid.equals(tid)) {
                    list.add(index.pid);
                }
            }
            for (PageId pid : list) {
                releaseLock(tid, pid);
            }
            return list;
        }
    }

    public boolean hasLockOn(TransactionId tid, PageId pid) {
        return lockTable.containsKey(new TableIndex(tid, pid));
    }

    private void registerLock(PageId pid) {
        if (!lockRegister.containsKey(pid))
            lockRegister.put(pid, new StampedLock());
    }

    private void checkPriority(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
        for (LockTuple tuple : lockTable.values()) {
            if (tuple.pid.equals(pid)
                    && (perm != Permissions.READ_ONLY
                    && tuple.perm != Permissions.READ_ONLY)) {
                if (tuple.tid.getId() < tid.getId()) {
                    //releaseAllLock(tid);
                    Debug.log("Abort Transaction: ", tid);
                    throw new TransactionAbortedException();
                }

            }
        }
    }

    public void convertCheck(TransactionId tid, PageId pid) throws
            TransactionAbortedException {
        long count = 0;
        for (LockTuple tuple : lockTable.values()) {
            if (tuple.pid.equals(pid))
                count++;
        }
        if (count > 1)
            throw new TransactionAbortedException();
    }

    private void acquireReadLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        TableIndex index = new TableIndex(tid, pid);
        if (!lockTable.containsKey(index)) {
            checkPriority(tid, pid, Permissions.READ_ONLY);
            StampedLock lock = lockRegister.get(pid);
            Long stamp = lock.readLock();
            lockTable.put(index, new LockTuple(tid, pid, stamp, Permissions.READ_ONLY));
            counter.add(pid);
        }
    }

    private void acquireWriteLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        TableIndex index = new TableIndex(tid, pid);
        if (lockTable.containsKey(index)) {
            if (lockTable.get(index).perm == Permissions.READ_ONLY) {
                //checkPriority(tid, pid, Permissions.READ_WRITE);
                convertCheck(tid, pid);
                StampedLock lock = lockRegister.get(pid);
                long stamp = lock.tryConvertToWriteLock(lockTable.get(index).stamp);
                while (stamp == 0) {
                    lock.unlockRead(lockTable.get(index).stamp);
                    stamp = lock.writeLock();
                }
                lockTable.put(index,new LockTuple(tid, pid, stamp, Permissions.READ_WRITE));
            }
        } else {
            checkPriority(tid, pid, Permissions.READ_WRITE);
            StampedLock lock = lockRegister.get(pid);
            Long stamp = lock.writeLock();
            lockTable.put(index, new LockTuple(tid, pid, stamp, Permissions.READ_WRITE));
            counter.add(pid);
        }
    }
}
