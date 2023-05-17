package io.delta.kernel.internal;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.fs.Path;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

public class SnapshotImpl implements Snapshot
{
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

        this.logReplay = new LogReplay(
                logPath,
                tableImpl.tableClient,
                logSegment);
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
            tableImpl.tableClient
        );
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
