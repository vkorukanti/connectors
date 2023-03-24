package io.delta.core.internal.checkpoint;

import io.delta.core.data.Row;
import io.delta.core.types.LongType;
import io.delta.core.types.StructType;

public class CheckpointMetaData {

    public static CheckpointMetaData fromRow(Row row) {
        return null;
    }

    public static StructType READ_SCHEMA = new StructType()
        .add("version", new LongType())
        .add("size", new LongType());

    public final long version;
    public final long size;

    public CheckpointMetaData(long version, long size) {
        this.version = version;
        this.size = size;
    }
}
