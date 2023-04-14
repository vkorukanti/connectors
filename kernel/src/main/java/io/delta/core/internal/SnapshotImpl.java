package io.delta.core.internal;

import io.delta.core.ScanBuilder;
import io.delta.core.ScanState;
import io.delta.core.Snapshot;
import io.delta.core.fs.Path;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.internal.actions.Metadata;
import io.delta.core.internal.actions.Protocol;
import io.delta.core.internal.lang.Lazy;
import io.delta.core.internal.lang.Tuple2;
import io.delta.core.internal.replay.LogReplay;
import io.delta.core.internal.snapshot.LogSegment;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public class SnapshotImpl implements Snapshot {
    private final Path logPath;
    private final Path dataPath;
    private final long version;
    private final LogSegment logSegment;
    private final TableImpl tableImpl;
    private final long timestamp;

    private final LogReplay logReplay;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;

    public SnapshotImpl(
            Path logPath,
            Path dataPath,
            long version,
            LogSegment logSegment,
            TableImpl tableImpl,
            long timestamp) {
        this.logPath = logPath;
        this.dataPath = dataPath;
        this.version = version;
        this.logSegment = logSegment;
        this.tableImpl = tableImpl;
        this.timestamp = timestamp;

        this.logReplay = new LogReplay(logPath, tableImpl.tableHelper, logSegment);
        this.protocolAndMetadata = logReplay.lazyLoadProtocolAndMetadata();
    }

    ////////////////////////////////////////
    // Public APIs
    ////////////////////////////////////////

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public StructType getSchema() {
        return getMetadata().getSchema();
    }

    @Override
    public ScanBuilder getScanBuilder() {
        return new ScanBuilderImpl(
            dataPath,
            protocolAndMetadata,
            getSchema(),
            getMetadata().getPartitionSchema(),
            logReplay.getAddFiles(),
            tableImpl.tableHelper
        );
    }

    @Override
    public ScanState getScanState()
    {
        return new ScanStateImpl(tableImpl.tableHelper);
    }

    ////////////////////////////////////////
    // Internal APIs
    ////////////////////////////////////////

    public Metadata getMetadata() {
        return protocolAndMetadata.get()._2;
    }

    public CloseableIterator<AddFile> getAddFiles() {
        return logReplay.getAddFiles();
    }

    public Protocol getProtocol() {
        return protocolAndMetadata.get()._1;
    }
}
