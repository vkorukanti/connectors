package io.delta.core.internal;

import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import io.delta.core.Snapshot;
import io.delta.core.Table;
import io.delta.core.TableVersionNotFoundException;
import io.delta.core.helpers.ScanFileContext;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.checkpoint.Checkpointer;
import io.delta.core.fs.Path;
import io.delta.core.internal.snapshot.SnapshotManager;
import io.delta.core.internal.util.Logging;

public class TableImpl implements Table, Logging {

    public static Table forPath(String path, TableHelper helper) {
        // TODO: take in a configuration and use conf.get("fs.defaultFS")
        final URI defaultUri = URI.create("file:///");
        final Path workingDir = new Path(Paths.get(".").toAbsolutePath().toUri());

        final Path dataPath = new Path(path).makeQualified(defaultUri, workingDir);
        final Path logPath = new Path(dataPath, "_delta_log");

        return new TableImpl(logPath, dataPath, helper);
    }

    public final Path logPath;
    public final Path dataPath;
    public final TableHelper tableHelper;
    public final Checkpointer checkpointer;
    public final SnapshotManager snapshotManager;

    private final ReentrantLock lock;

    public TableImpl(
            Path logPath,
            Path dataPath,
            TableHelper tableHelper) {
        logDebug(
            String.format("TableImpl created with logPath %s, dataPath %s", logPath, dataPath)
        );

        this.logPath = logPath;
        this.dataPath = dataPath;
        this.tableHelper = tableHelper;

        this.lock = new ReentrantLock();
        this.checkpointer = new Checkpointer(this);
        this.snapshotManager = new SnapshotManager(this);
    }

    @Override
    public Snapshot getSnapshotAtVersion(long version)
            throws TableVersionNotFoundException
    {
        throw new UnsupportedOperationException("Not yet implemented");
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
