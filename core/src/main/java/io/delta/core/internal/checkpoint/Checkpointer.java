package io.delta.core.internal.checkpoint;

import java.util.*;
import java.util.stream.Collectors;

import io.delta.core.data.Row;
import io.delta.core.internal.TableImpl;
import io.delta.core.utils.CloseableIterator;

public class Checkpointer {

    ////////////////////
    // Static Methods //
    ////////////////////

    /** The name of the last checkpoint file */
    public static final String LAST_CHECKPOINT_FILE_NAME = "_last_checkpoint";

    /**
     * Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
     * later than `notLaterThan`.
     */
    public static Optional<CheckpointInstance> getLatestCompleteCheckpointFromList(
            List<CheckpointInstance> instances,
            CheckpointInstance notLaterThan) {
        final List<CheckpointInstance> completeCheckpoints = instances
            .stream()
            .filter(c -> c.isNotLaterThan(notLaterThan))
            .collect(Collectors.groupingBy(c -> c))
            // Map<CheckpointInstance, List<CheckpointInstance>>
            .entrySet()
            .stream()
            .filter(entry -> {
                final CheckpointInstance key = entry.getKey();
                final List<CheckpointInstance> inst = entry.getValue();

                if (key.numParts.isPresent()) {
                    return inst.size() == entry.getKey().numParts.get();
                } else {
                    return inst.size() == 1;
                }
            })
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        if (completeCheckpoints.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(Collections.max(completeCheckpoints));
        }
    }

    ///////////////////////////////
    // Instance Fields / Methods //
    ///////////////////////////////

    /** The path to the file that holds metadata about the most recent checkpoint. */
    private final String LAST_CHECKPOINT;

    private final TableImpl tableImpl;

    public Checkpointer(TableImpl tableImpl) {
        this.tableImpl = tableImpl;

        this.LAST_CHECKPOINT = tableImpl.logPath + "/" + LAST_CHECKPOINT_FILE_NAME;
    }

    /** Returns information about the most recent checkpoint. */
    public Optional<CheckpointMetaData> readLastCheckpointFile() {
        return loadMetadataFromFile(0);
    }

    /** Loads the checkpoint metadata from the _last_checkpoint file. */
    private Optional<CheckpointMetaData> loadMetadataFromFile(int tries) {
        final CloseableIterator<Row> data1 = tableImpl
            .tableHelper
            .readJsonFile(LAST_CHECKPOINT, CheckpointMetaData.READ_SCHEMA);

        return Optional.of(CheckpointMetaData.fromRow(data1.next()));
    }
}
