package io.delta.core.internal.snapshot;

import io.delta.core.fs.Path;
import io.delta.core.internal.SnapshotImpl;
import io.delta.core.internal.TableImpl;

public class InitialSnapshot extends SnapshotImpl {
    public InitialSnapshot(Path logPath, TableImpl tableImpl) {
        super(logPath, -1 /* version */, LogSegment.empty(logPath), tableImpl, -1 /* timestamp */);
    }
}
