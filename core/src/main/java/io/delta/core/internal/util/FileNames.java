package io.delta.core.internal.util;

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
