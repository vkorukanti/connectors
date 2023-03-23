package io.delta.core.internal.checkpoint;

import io.delta.core.data.Row;
import io.delta.core.helpers.TypeReference;
import io.delta.core.types.StructType;

public class CheckpointMetaData {

    // DECISION 1 OPTION 1
    public static CheckpointMetaData fromRow(Row row) {
        return null;
    }
    // DECISION 1 OPTION 2
    public static TypeReference<CheckpointMetaData> TYPE_REFERENCE =
        new TypeReference<CheckpointMetaData>() { };

    public static StructType READ_SCHEMA = null;

    public final long version;
    public final long size;

    public CheckpointMetaData(long version, long size) {
        this.version = version;
        this.size = size;
    }
}
