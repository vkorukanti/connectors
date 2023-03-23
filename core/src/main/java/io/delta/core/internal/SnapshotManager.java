package io.delta.core.internal;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

import io.delta.core.Snapshot;
import io.delta.core.fs.FileStatus;
import io.delta.core.internal.checkpoint.CheckpointInstance;
import io.delta.core.internal.checkpoint.CheckpointMetaData;
import io.delta.core.internal.checkpoint.Checkpointer;
import io.delta.core.internal.lang.ListUtils;
import io.delta.core.internal.lang.Tuple2;
import io.delta.core.internal.util.FileNames;
import io.delta.core.utils.CloseableIterator;

public class SnapshotManager {

    ////////////////////
    // Static Methods //
    ////////////////////

    /**
     * - Verify the versions are contiguous.
     * - Verify the versions start with `expectedStartVersion` if it's specified.
     * - Verify the versions end with `expectedEndVersion` if it's specified.
     */
    public static void verifyDeltaVersions(
            List<Long> versions,
            Optional<Long> expectedStartVersion,
            Optional<Long> expectedEndVersion) {
        if (!versions.isEmpty()) {
            // TODO(SCOTT): check if contiguous
        }
        expectedStartVersion.ifPresent(v -> {
            assert (!versions.isEmpty() && Objects.equals(versions.get(0), v)) :
                String.format("Did not get the first delta file version %s to compute Snapshot", v);
        });
        expectedEndVersion.ifPresent(v -> {
            assert (!versions.isEmpty() && Objects.equals(versions.get(versions.size() - 1), v)) :
                String.format("Did not get the last delta file version %s to compute Snapshot", v);
        });
    }

    ///////////////////////////////
    // Instance Fields / Methods //
    ///////////////////////////////

    private final TableImpl tableImpl;
    volatile private Snapshot currentSnapshot;

    public SnapshotManager(TableImpl tableImpl) {
        this.tableImpl = tableImpl;
        this.currentSnapshot = getSnapshotAtInit();
    }

    /////////////////
    // Public APIs //
    /////////////////

    /**
     * Update current snapshot by applying the new delta files if any.
     */
    public Snapshot update() {

    }

    //////////////////
    // Private APIs //
    //////////////////

    /** Get an iterator of files in the _delta_log directory starting with the startVersion. */
    private CloseableIterator<FileStatus> listFrom(long startVersion) throws FileNotFoundException {
        return tableImpl
            .tableHelper
            .listFiles(FileNames.listingPrefix(tableImpl.logPath, startVersion));
    }

    /** Returns true if the path is delta log files. Delta log files can be delta commit file
     * (e.g., 000000000.json), or checkpoint file. (e.g., 000000001.checkpoint.00001.00003.parquet)
     *
     * @param path Path of a file
     * @return Boolean Whether the file is delta log files
     */
    private boolean isDeltaCommitOrCheckpointFile(String path) {
        return FileNames.isCheckpointFile(path) || FileNames.isDeltaFile(path);
    }

    /** Returns an iterator containing a list of files found from the provided path */
    private Optional<CloseableIterator<FileStatus>> listFromOrNone(long startVersion) {
        // LIST the directory, starting from the provided lower bound (treat missing dir as empty).
        // NOTE: "empty/missing" is _NOT_ equivalent to "contains no useful commit files."
        try {
            // TODO: would be great to build our own better Optional class, e.g. Option.filterNot
            CloseableIterator<FileStatus> results = listFrom(startVersion);
            if (results.hasNext()) {
                return Optional.of(results);
            } else {
                return Optional.empty();
            }
        } catch (FileNotFoundException e) {
            return Optional.empty();
        }
    }

    /**
     * Returns the delta files and checkpoint files starting from the given `startVersion`.
     * `versionToLoad` is an optional parameter to set the max bound. It's usually used to load a
     * table snapshot for a specific version.
     *
     * @param startVersion the version to start. Inclusive.
     * @param versionToLoad the optional parameter to set the max version we should return. Inclusive.
     * @return Some array of files found (possibly empty, if no usable commit files are present), or
     *         None if the listing returned no files at all.
     */
    protected final Optional<List<FileStatus>> listDeltaAndCheckpointFiles(
            long startVersion,
            Optional<Long> versionToLoad) {
        return listFromOrNone(startVersion).map(fileStatusesIter -> {
            final List<FileStatus> output = new ArrayList<>();

            while(fileStatusesIter.hasNext()) {
                final FileStatus fileStatus = fileStatusesIter.next();

                // Pick up all checkpoint and delta files
                if (!isDeltaCommitOrCheckpointFile(fileStatus.path())) {
                    continue;
                }

                // Checkpoint files of 0 size are invalid but may be ignored silently when read,
                // hence we drop them so that we never pick up such checkpoints.
                if (FileNames.isCheckpointFile(fileStatus.path()) && fileStatus.length() == 0) {
                    continue;
                }

                // Take files until the version we want to load
                final boolean versionWithinRange = versionToLoad
                    .map(v -> FileNames.getFileVersion(fileStatus.path()) < v)
                    .orElse(false);

                if (!versionWithinRange) {
                    break;
                }

                output.add(fileStatus);
            }

            return output;
        });
    }

    /**
     * Load the Snapshot for this Delta table at initialization. This method uses the
     * `lastCheckpoint` file as a hint on where to start listing the transaction log directory. If
     * the _delta_log directory doesn't exist, this method will return an `InitialSnapshot`.
     */
    private Snapshot getSnapshotAtInit() {
        final long currentTimestamp = System.currentTimeMillis();
        final Optional<CheckpointMetaData> lastCheckpointOpt =
            tableImpl.checkpointer.readLastCheckpointFile();
        getLogSegmentFrom(lastCheckpointOpt);
    }

    /**
     * Get the LogSegment that will help in computing the Snapshot of the table at DeltaLog
     * initialization, or None if the directory was empty/missing.
     *
     * @param startingCheckpoint A checkpoint that we can start our listing from
     */
    private Optional<LogSegment> getLogSegmentFrom(
            Optional<CheckpointMetaData> startingCheckpoint) {
        return getLogSegmentForVersion(startingCheckpoint.map(x -> x.version), Optional.empty());
    }

    /**
     * Get a list of files that can be used to compute a Snapshot at version `versionToLoad`, If
     * `versionToLoad` is not provided, will generate the list of files that are needed to load the
     * latest version of the Delta table. This method also performs checks to ensure that the delta
     * files are contiguous.
     *
     * @param startCheckpoint A potential start version to perform the listing of the DeltaLog,
     *                        typically that of a known checkpoint. If this version's not provided,
     *                        we will start listing from version 0.
     * @param versionToLoad A specific version to load. Typically used with time travel and the
     *                      Delta streaming source. If not provided, we will try to load the latest
     *                      version of the table.
     * @return Some LogSegment to build a Snapshot if files do exist after the given
     *         startCheckpoint. None, if the directory was missing or empty.
     */
    private Optional<LogSegment> getLogSegmentForVersion(
            Optional<Long> startCheckpoint,
            Optional<Long> versionToLoad) {
        // List from the starting checkpoint. If a checkpoint doesn't exist, this will still return
        // deltaVersion=0.
        final Optional<List<FileStatus>> newFiles =
            listDeltaAndCheckpointFiles(startCheckpoint.orElse(0L), versionToLoad);
        return getLogSegmentForVersion(startCheckpoint, versionToLoad, newFiles);
    }

    /**
     * Helper function for the getLogSegmentForVersion above. Called with a provided files list,
     * and will then try to construct a new LogSegment using that.
     */
    private Optional<LogSegment> getLogSegmentForVersion(
            final Optional<Long> startCheckpointOpt,
            Optional<Long> versionToLoadOpt,
            Optional<List<FileStatus>> filesOpt) {
        final List<FileStatus> newFiles;
        if (filesOpt.isPresent()) {
            newFiles = filesOpt.get();
        } else {
            // No files found even when listing from 0 => empty directory => table does not exist yet.
            if (!startCheckpointOpt.isPresent()) return null;

            // FIXME(ryan.johnson): We always write the commit and checkpoint files before updating
            //  _last_checkpoint. If the listing came up empty, then we either encountered a
            // list-after-put inconsistency in the underlying log store, or somebody corrupted the
            // table by deleting files. Either way, we can't safely continue.
            //
            // For now, we preserve existing behavior by returning Array.empty, which will trigger a
            // recursive call to [[getLogSegmentForVersion]] below (same as before the refactor).
            newFiles = Collections.emptyList();
        }

        if (newFiles.isEmpty() && !startCheckpointOpt.isPresent()) {
            // We can't construct a snapshot because the directory contained no usable commit
            // files... but we can't return Optional.empty either, because it was not truly empty.
            throw new RuntimeException("Empty directory");
        } else if (newFiles.isEmpty()) {
            // The directory may be deleted and recreated and we may have stale state in our DeltaLog
            // singleton, so try listing from the first version
            return getLogSegmentForVersion(Optional.empty(), versionToLoadOpt);
        }

        Tuple2<List<FileStatus>, List<FileStatus>> checkpointsAndDeltas = ListUtils
            .partition(
                newFiles,
                fileStatus -> FileNames.isCheckpointFile(fileStatus.path())
            );
        final List<FileStatus> checkpoints = checkpointsAndDeltas._1;
        final List<FileStatus> deltas = checkpointsAndDeltas._2;

        // Find the latest checkpoint in the listing that is not older than the versionToLoad
        final CheckpointInstance lastCheckpoint = versionToLoadOpt.map(CheckpointInstance::new)
            .orElse(CheckpointInstance.MAX_VALUE);
        final List<CheckpointInstance> checkpointFiles = checkpoints
            .stream()
            .map(f -> new CheckpointInstance(f.path()))
            .collect(Collectors.toList());
        final Optional<CheckpointInstance> newCheckpointOpt =
            Checkpointer.getLatestCompleteCheckpointFromList(checkpointFiles, lastCheckpoint);
        final long newCheckpointVersion = newCheckpointOpt
            .map(c -> c.version)
            .orElseGet(() -> {
                // If we do not have any checkpoint, pass new checkpoint version as -1 so that first
                // delta version can be 0.
                startCheckpointOpt.map(startCheckpoint -> {
                    // `startCheckpointOpt` was given but no checkpoint found on delta log. This means that the
                    // last checkpoint we thought should exist (the `_last_checkpoint` file) no longer exists.
                    // Try to look up another valid checkpoint and create `LogSegment` from it.
                    //
                    // FIXME(ryan.johnson): Something has gone very wrong if the checkpoint doesn't
                    // exist at all. This code should only handle rejected incomplete checkpoints.
                    final long snapshotVersion = versionToLoadOpt.orElseGet(() -> {
                        final FileStatus lastDelta = deltas.get(deltas.size() - 1);
                        return FileNames.deltaVersion(lastDelta.path());
                    });

                    return getLogSegmentWithMaxExclusiveCheckpointVersion(snapshotVersion, startCheckpoint)
                        .orElseThrow(() ->
                            // No alternative found, but the directory contains files so we cannot return None.
                            new RuntimeException(
                                String.format("Checkpoint file to load version: %s is missing.", startCheckpoint)
                            )
                        );

                });

                return -1L;
            });

        // TODO(SCOTT): we can calculate deltasAfterCheckpoint and deltaVersions more efficiently

        // If there is a new checkpoint, start new lineage there. If `newCheckpointVersion` is -1,
        // it will list all existing delta files.
        final List<FileStatus> deltasAfterCheckpoint = deltas
            .stream()
            .filter(fileStatus -> FileNames.deltaVersion(fileStatus.path()) > newCheckpointVersion)
            .collect(Collectors.toList());

        final LinkedList<Long> deltaVersions = deltas
            .stream()
            .map(fileStatus -> FileNames.deltaVersion(fileStatus.path()))
            .collect(Collectors.toCollection(LinkedList::new));

        // We may just be getting a checkpoint file after the filtering
        if (!deltaVersions.isEmpty()) {
            if (deltaVersions.get(0) != newCheckpointVersion + 1) {
                throw new RuntimeException("log file not found excpetion");
            }
            verifyDeltaVersions(deltaVersions, Optional.of(newCheckpointVersion + 1), versionToLoadOpt);
        }

        final long newVersion = deltaVersions.isEmpty() ? newCheckpointOpt.get().version : deltaVersions.getLast();
        final List<FileStatus> newCheckpointFiles


        val newVersion = deltaVersions.lastOption.getOrElse(newCheckpoint.get.version)
        val newCheckpointFiles: Seq[FileStatus] = newCheckpoint.map { newCheckpoint =>
            val newCheckpointPaths = newCheckpoint.getCorrespondingFiles(logPath).toSet
            val newCheckpointFileArray = checkpoints.filter(f => newCheckpointPaths.contains(f.getPath))
            assert(newCheckpointFileArray.length == newCheckpointPaths.size,
            "Failed in getting the file information for:\n" +
                newCheckpointPaths.mkString(" -", "\n -", "") + "\n" +
                "among\n" + checkpoints.map(_.getPath).mkString(" -", "\n -", ""))
            newCheckpointFileArray.toSeq
        }.getOrElse(Nil)
    }

    /**
     * Returns a [[LogSegment]] for reading `snapshotVersion` such that the segment's checkpoint
     * version (if checkpoint present) is LESS THAN `maxExclusiveCheckpointVersion`.
     * This is useful when trying to skip a bad checkpoint. Returns `None` when we are not able to
     * construct such [[LogSegment]], for example, no checkpoint can be used but we don't have the
     * entire history from version 0 to version `snapshotVersion`.
     */
    private Optional<LogSegment> getLogSegmentWithMaxExclusiveCheckpointVersion(
            long snapshotVersion,
            long maxExclusiveCheckpointVersion) {
        // TODO
        return Optional.empty();
    }
}
