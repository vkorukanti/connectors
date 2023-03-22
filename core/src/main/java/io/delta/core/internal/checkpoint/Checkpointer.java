package io.delta.core.internal.checkpoint;

import java.util.Optional;

import io.delta.core.internal.TableImpl;

public class Checkpointer {
    private final TableImpl tableImpl;

    public Checkpointer(TableImpl tableImpl) {
        this.tableImpl = tableImpl;
    }

    /** Returns information about the most recent checkpoint. */
    public Optional<CheckpointMetaData> readLastCheckpointFile() {
        return loadMetadataFromFile(0);
    }

    /** Loads the checkpoint metadata from the _last_checkpoint file. */
    private Optional<CheckpointMetaData> loadMetadataFromFile(int tries) {

    }
}
