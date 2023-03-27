package io.delta.core.internal.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FileNames {

    private FileNames() { }

    private static final Pattern DELTA_FILE_PATTERN =
        Pattern.compile("\\d+\\.json");

    private static final Pattern CHECKPOINT_FILE_PATTERN =
        Pattern.compile("\\d+\\.checkpoint(\\.\\d+\\.\\d+)?\\.parquet");

    /** Returns the delta (json format) path for a given delta file. */
    public static String deltaFile(String path, long version) {
        return String.format("%s/%020d.json", path, version);
    }

    /** Returns the version for the given delta path. */
    public static long deltaVersion(String path) {
        return Long.parseLong(path.split("\\.")[0]);
    }

    /** Returns the version for the given checkpoint path. */
    public static long checkpointVersion(String path) {
        return Long.parseLong(path.split("\\.")[0]);
    }

    /**
     * Returns the prefix of all delta log files for the given version.
     *
     * Intended for use with listFrom to get all files from this version onwards. The returned Path
     * will not exist as a file.
     */
    public static String listingPrefix(final String path, final long version) {
        return String.format("%s/%020d.", path, version);
    }

    /**
     * Returns the path for a singular checkpoint up to the given version.
     *
     * In a future protocol version this path will stop being written.
     */
    public static String checkpointFileSingular(String path, long version) {
        return String.format("%s/%020d.checkpoint.parquet", path, version);
    }

    /**
     * Returns the paths for all parts of the checkpoint up to the given version.
     *
     * In a future protocol version we will write this path instead of checkpointFileSingular.
     *
     * Example of the format: 00000000000000004915.checkpoint.0000000020.0000000060.parquet is
     * checkpoint part 20 out of 60 for the snapshot at version 4915. Zero padding is for
     * lexicographic sorting.
     */
    public static List<String> checkpointFileWithParts(String path, long version, int numParts) {
        final List<String> output = new ArrayList<>();
        for (int i = 1; i < numParts + 1; i++) {
            output.add(String.format("%s/%020d.checkpoint.%010d.%010d.parquet", path, i, numParts, version));
        }
        return output;
    }

    public static boolean isCheckpointFile(final String path) {
        return CHECKPOINT_FILE_PATTERN.matcher(path).find();
    }

    public static boolean isDeltaFile(final String path) {
        return DELTA_FILE_PATTERN.matcher(path).find();
    }

    /**
     * Get the version of the checkpoint, checksum or delta file. Throws an error if an unexpected
     * file type is seen. These unexpected files should be filtered out to ensure forward
     * compatibility in cases where new file types are added, but without an explicit protocol
     * upgrade.
     */
    public static long getFileVersion(String path) {
        if (isCheckpointFile(path)) {
            return checkpointVersion(path);
        } else if (isDeltaFile(path)) {
            return deltaVersion(path);
//        } else if (isChecksumFile(path)) {
//            checksumVersion(path)
        } else {
            throw new AssertionError(
                String.format("Unexpected file type found in transaction log: %s", path)
            );
        }
    }
}
