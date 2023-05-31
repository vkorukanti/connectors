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
    public static Table forPath(String path) {
        // TODO: take in a configuration and use conf.get("fs.defaultFS")
        final URI defaultUri = URI.create("file:///");
        final Path workingDir = new Path(Paths.get(".").toAbsolutePath().toUri());

        final Path dataPath = new Path(path).makeQualified(defaultUri, workingDir);
        final Path logPath = new Path(dataPath, "_delta_log");

        return new TableImpl(logPath, dataPath);
    }

    public final Path logPath;
    public final Path dataPath;
    public final Checkpointer checkpointer;
    public final SnapshotManager snapshotManager;

    public TableImpl(Path logPath, Path dataPath) {
        logDebug(
                String.format("TableImpl created with logPath %s, dataPath %s", logPath, dataPath));

        this.logPath = logPath;
        this.dataPath = dataPath;

        this.checkpointer = new Checkpointer(dataPath.toString());
        this.snapshotManager = new SnapshotManager();
    }

    @Override
    public Snapshot getLatestSnapshot(TableClient tableClient) {
        return snapshotManager.update(tableClient, this);
    }
}
