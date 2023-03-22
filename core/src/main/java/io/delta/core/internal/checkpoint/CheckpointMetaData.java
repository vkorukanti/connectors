package io.delta.core.internal.checkpoint;

public class CheckpointMetaData {
    public final long version;
    public final long size;

    public CheckpointMetaData(long version, long size) {
        this.version = version;
        this.size = size;
    }
}
