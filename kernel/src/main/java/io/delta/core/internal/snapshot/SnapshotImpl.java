package io.delta.core.internal.snapshot;

import io.delta.core.ScanBuilder;
import io.delta.core.Snapshot;
import io.delta.core.fs.Path;
import io.delta.core.internal.LogSegment;
import io.delta.core.internal.TableImpl;
import io.delta.core.internal.replay.LogReplay;
import io.delta.core.types.StructType;

public class SnapshotImpl implements Snapshot {
    private final Path logPath;
    private final long version;
    private final LogSegment logSegment;
    private final TableImpl tableImpl;
    private final long timestamp;

    private final LogReplay logReplay;

    public SnapshotImpl(
            Path logPath,
            long version,
            LogSegment logSegment,
            TableImpl tableImpl,
            long timestamp) {
        this.logPath = logPath;
        this.version = version;
        this.logSegment = logSegment;
        this.tableImpl = tableImpl;
        this.timestamp = timestamp;

        this.logReplay = new LogReplay(logPath, tableImpl.tableHelper, logSegment);

    }

    @Override
    public long getVersion() {
        return version;
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
