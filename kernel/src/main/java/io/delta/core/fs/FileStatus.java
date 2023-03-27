package io.delta.core.fs;

public interface FileStatus {

    default Path path() {
        return new Path(pathStr());
    }

    String pathStr();

    long length();

    long modificationTime();
}
