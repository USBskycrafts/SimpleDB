package simpledb.transaction;

import simpledb.common.Debug;
import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.HashSet;

public class WaitForGraph {
    private final HashMap<TransactionId, HashMap<PageId, TransactionId>> nTable;

    private final HashSet<TransactionId> visited;

    public WaitForGraph() {
        nTable = new HashMap<>();
        visited = new HashSet<>();
    }

    public void add(TransactionId tid, PageId pid) {
        for (HashMap<PageId, TransactionId> edge : nTable.values()) {
            if (edge.containsKey(pid))
                add(edge.get(pid), tid, pid);
        }
    }


    private void add(TransactionId dst, TransactionId src, PageId pid) {
        if (!nTable.containsKey(src))
            nTable.put(src, new HashMap<>());
        nTable.get(src).put(pid, dst);
    }

    public void delete(TransactionId tid, PageId pid) {
        if (!nTable.containsKey(tid))
            return;
        for (TransactionId src : nTable.keySet()) {
            if (nTable.get(src).containsKey(pid) && nTable.get(src).get(pid).equals(tid)) {
                nTable.get(src).put(pid, nTable.get(tid).get(pid));
            }
        }
        nTable.get(tid).remove(pid);
    }

    public void deleteAll(TransactionId tid) {
        if (!nTable.containsKey(tid))
            return;
        for (PageId pid : nTable.get(tid).keySet()) {
            delete(tid, pid);
        }
        assert nTable.get(tid).isEmpty();
        nTable.remove(tid);
    }

    public boolean hasCycle() {
        visited.clear();
        for (TransactionId tid : nTable.keySet()) {
            if (hasCycle(tid))
                return true;
            visited.clear();
        }
        return false;
    }

    private boolean hasCycle(TransactionId tid) {
        if (visited.contains(tid))
            return true;
        visited.add(tid);
        for (TransactionId v : nTable.get(tid).values()) {
            Debug.log("Transaction: ", tid, " ", v);
            if (hasCycle(v))
                return true;
        }
        return false;
    }
}
