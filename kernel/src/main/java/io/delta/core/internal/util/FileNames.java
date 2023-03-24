package io.delta.core.internal.util;

import java.util.ArrayList;
import java.util.List;

public final class FileNames {

    private FileNames() { }

    /** Returns the version for the given delta path. */
    public static long deltaVersion(String path) {
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
        return false;
    }

    public static boolean isDeltaFile(final String path) {
        return false;
    }

    /**
     * Get the version of the checkpoint, checksum or delta file. Throws an error if an unexpected
     * file type is seen. These unexpected files should be filtered out to ensure forward
     * compatibility in cases where new file types are added, but without an explicit protocol
     * upgrade.
     */
    public static long getFileVersion(String path) {
        return 101;
//        if (isCheckpointFile(path)) {
//            checkpointVersion(path)
//        } else if (isDeltaFile(path)) {
//            deltaVersion(path)
//        } else if (isChecksumFile(path)) {
//            checksumVersion(path)
//        } else {
//            // scalastyle:off throwerror
//            throw new AssertionError(
//                s"Unexpected file type found in transaction log: $path")
//            // scalastyle:on throwerror
//        }
    }
}
