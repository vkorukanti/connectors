package io.delta.core.internal;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import io.delta.core.Snapshot;
import io.delta.core.Table;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.checkpoint.Checkpointer;

public class TableImpl implements Table {

    public static Table forPath(String path, TableHelper helper) {
        final String logPath = path + "/_delta_log"; // TODO: FileUtils, makeQualified, etc.

        return new TableImpl(logPath, path, helper);
    }

    public final String logPath;
    public final String dataPath;
    public final TableHelper tableHelper;
    public final Checkpointer checkpointer;
    public final SnapshotManager snapshotManager;

    private final ReentrantLock lock;

    public TableImpl(
            String logPath,
            String dataPath,
            TableHelper tableHelper) {
        this.logPath = logPath;
        this.dataPath = dataPath;
        this.tableHelper = tableHelper;

        this.lock = new ReentrantLock();
        this.checkpointer = new Checkpointer(this);
        this.snapshotManager = new SnapshotManager(this);
    }

    @Override
    public Snapshot getLatestSnapshot() {
        return snapshotManager.update();
    }

    public <T> T lockInterruptibly(Callable<T> body) {
        try {
            lock.lockInterruptibly();

            try {
                return body.call();
            } catch (Exception e) {
                // failed body.call()
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            // failed lock.lockInterruptibly();
            throw new RuntimeException(e);
        }
    }
}
