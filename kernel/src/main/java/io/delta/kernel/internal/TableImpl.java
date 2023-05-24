package io.delta.kernel.internal;

import java.net.URI;
import java.nio.file.Paths;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableVersionNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.internal.checkpoint.Checkpointer;
import io.delta.kernel.fs.Path;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.internal.util.Logging;

public class TableImpl implements Table, Logging
{
    public static Table forPath(TableClient client, String path) {
        // TODO: take in a configuration and use conf.get("fs.defaultFS")
        final URI defaultUri = URI.create("file:///");
        final Path workingDir = new Path(Paths.get(".").toAbsolutePath().toUri());

        final Path dataPath = new Path(path).makeQualified(defaultUri, workingDir);
        final Path logPath = new Path(dataPath, "_delta_log");

        return new TableImpl(logPath, dataPath, client);
    }

    public final Path logPath;
    public final Path dataPath;
    public final TableClient tableClient;
    public final Checkpointer checkpointer;
    public final SnapshotManager snapshotManager;

    public TableImpl(
            Path logPath,
            Path dataPath,
            TableClient tableClient) {
        logDebug(
                String.format("TableImpl created with logPath %s, dataPath %s", logPath, dataPath));

        this.logPath = logPath;
        this.dataPath = dataPath;
        this.tableClient = tableClient;

        this.checkpointer = new Checkpointer(this);
        this.snapshotManager = new SnapshotManager(this);
    }

    @Override
    public Snapshot getSnapshotAtVersion(TableClient tableClient, long version)
            throws TableVersionNotFoundException
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Snapshot getLatestSnapshot(TableClient tableClient) {
        // TODO: update to use the tableClient that is passed to this function
        return snapshotManager.update();
    }
}
