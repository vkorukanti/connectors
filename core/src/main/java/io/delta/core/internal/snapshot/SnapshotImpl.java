package io.delta.core.internal.snapshot;

import io.delta.core.ScanBuilder;
import io.delta.core.Snapshot;
import io.delta.core.internal.LogSegment;
import io.delta.core.internal.TableImpl;
import io.delta.core.types.StructType;

public class SnapshotImpl implements Snapshot {
    private final String path;
    private final long version;
    private final LogSegment logSegment;
    private final TableImpl tableImpl;
    private final long timestamp;

    public SnapshotImpl(
            String path,
            long version,
            LogSegment logSegment,
            TableImpl tableImpl,
            long timestamp) {
        this.path = path;
        this.version = version;
        this.logSegment = logSegment;
        this.tableImpl = tableImpl;
        this.timestamp = timestamp;
    }

    @Override
    public StructType getSchema() {
        return null;
    }

    @Override
    public ScanBuilder getScanBuilder() {
        return null;
    }
}
