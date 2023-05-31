package io.delta.kernel.internal.snapshot;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.fs.Path;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableImpl;

public class InitialSnapshot extends SnapshotImpl {
    public InitialSnapshot(Path logPath, Path dataPath, TableClient tableClient) {
        super(
                logPath,
                dataPath,
                -1 /* version */,
                LogSegment.empty(logPath),
                tableClient,
                -1 /* timestamp */);
    }
}
