package io.delta.core.fs;

public class FileStatus {

    private final Path path;
    private final long length;
    private final long modificationTime;

    public FileStatus(String path, long length, long modificationTime) {
        this.path = new Path(path);
        this.length = length;
        this.modificationTime = modificationTime;
    }

    public Path getPath() {
        return path;
    }

    public long getLength() {
        return length;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public static FileStatus of(String path) {
        // TODO: make the length and modification optional
        return new FileStatus(path, 0, 0);
    }

    public static FileStatus of(String path, long size, long modificationTime) {
        return new FileStatus(path, size, modificationTime);
    }
}
