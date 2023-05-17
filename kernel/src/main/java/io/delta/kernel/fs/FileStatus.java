package io.delta.kernel.fs;

import java.util.Objects;

/**
 * Class for encapsulating metadata about a file in Delta Lake table.
 */
public class FileStatus {

    private final String path;
    private final long size;
    private final boolean hasDeletionVector;

    private FileStatus(String path, long size, boolean hasDeletionVector) {
        this.path = Objects.requireNonNull(path, "path is null");
        this.size = size; // TODO: validation
        this.hasDeletionVector = hasDeletionVector;
    }

    /**
     * Get the path to the file.
     * @return Fully qualified file path
     */
    public String getPath() {
        return path;
    }

    /**
     * Get the size of the file in bytes.
     * @return File size in bytes.
     */
    public long getSize()
    {
        return size;
    }

    /**
     * Whether this file has any associated deletion vector?
     * @return True if the file has an associated deletion vector. Otherwise false.
     */
    public boolean isHasDeletionVector()
    {
        return hasDeletionVector;
    }

    /**
     * Create a {@link FileStatus} representing the given path and file size.
     * @param path Fully qualified file path.
     * @param size File size in bytes
     * @return
     */
    public static FileStatus of(String path, long size) {
        return new FileStatus(path, size, false);
    }

    /**
     * Create a {@link FileStatus}.
     *
     * @param path Fully qualified file path.
     * @param size File size in bytes
     * @param hasDeletionVector Whether the file has an associated deletion vector file.
     * @return
     */
    public static FileStatus of(String path, long size, boolean hasDeletionVector) {
        return new FileStatus(path, size, hasDeletionVector);
    }
}
