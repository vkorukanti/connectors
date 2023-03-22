package io.delta.core.internal;

import java.util.Optional;

import io.delta.core.Snapshot;
import io.delta.core.internal.checkpoint.CheckpointMetaData;

public class SnapshotManager {

    private final TableImpl tableImpl;
    volatile private Snapshot currentSnapshot;

    public SnapshotManager(TableImpl tableImpl) {
        this.tableImpl = tableImpl;
        this.currentSnapshot = getSnapshotAtInit();
    }

    /**
     * Update current snapshot by applying the new delta files if any.
     */
    public Snapshot update() {

    }

    /**
     * Load the Snapshot for this Delta table at initialization. This method uses the
     * `lastCheckpoint` file as a hint on where to start listing the transaction log directory. If
     * the _delta_log directory doesn't exist, this method will return an `InitialSnapshot`.
     */
    private Snapshot getSnapshotAtInit() {
        final long currentTimestamp = System.currentTimeMillis();
        final Optional<CheckpointMetaData> lastCheckpointOpt =
            tableImpl.checkpointer.readLastCheckpointFile();
    }
}
