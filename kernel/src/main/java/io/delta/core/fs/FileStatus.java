package io.delta.core.fs;

public interface FileStatus {

    String path();

    long length();

    long modificationTime();
}
