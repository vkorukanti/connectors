package io.delta.core.internal.checkpoint;

import io.delta.core.data.Row;
import io.delta.core.types.LongType;
import io.delta.core.types.StructType;

public class CheckpointMetaData {

    public static CheckpointMetaData fromRow(Row row) {
        return new CheckpointMetaData(
            row.getLong(0),
            row.getLong(1)
        );
    }

    public static StructType READ_SCHEMA = new StructType()
        .add("version", LongType.INSTANCE)
        .add("size", LongType.INSTANCE);

    public final long version;
    public final long size;

    public CheckpointMetaData(long version, long size) {
        this.version = version;
        this.size = size;
    }

    @Override
    public String toString() {
        return "CheckpointMetaData{" +
            "version=" + version +
            ", size=" + size +
            '}';
    }
}
