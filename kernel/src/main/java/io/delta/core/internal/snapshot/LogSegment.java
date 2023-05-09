package io.delta.core.internal.snapshot;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.delta.core.fs.FileStatus;
import io.delta.core.fs.Path;

public class LogSegment {
    public final Path logPath;
    public final long version;
    public final List<FileStatus> deltas;
    public final List<FileStatus> checkpoints;
    public final Optional<Long> checkpointVersionOpt;
    public final long lastCommitTimestamp;

    public static LogSegment empty(Path logPath) {
        return new LogSegment(
            logPath,
            -1,
            Collections.emptyList(),
            Collections.emptyList(),
            Optional.empty(),
            -1
        );
    }

    /**
     * Provides information around which files in the transaction log need to be read to create
     * the given version of the log.
     *
     * @param logPath The path to the _delta_log directory
     * @param version The Snapshot version to generate
     * @param deltas The delta commit files (.json) to read
     * @param checkpoints The checkpoint file(s) to read
     * @param checkpointVersionOpt The checkpoint version used to start replay
     * @param lastCommitTimestamp The "unadjusted" timestamp of the last commit within this segment.
     *                            By unadjusted, we mean that the commit timestamps may not
     *                            necessarily be monotonically increasing for the commits within
     *                            this segment.
     */
    public LogSegment(
            Path logPath,
            long version,
            List<FileStatus> deltas,
            List<FileStatus> checkpoints,
            Optional<Long> checkpointVersionOpt,
            long lastCommitTimestamp) {
        this.logPath = logPath;
        this.version = version;
        this.deltas = deltas;
        this.checkpoints = checkpoints;
        this.checkpointVersionOpt = checkpointVersionOpt;
        this.lastCommitTimestamp = lastCommitTimestamp;
    }
}
