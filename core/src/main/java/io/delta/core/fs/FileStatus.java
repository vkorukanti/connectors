package io.delta.core.fs;

public interface FileStatus {

    String filePath();

    long size();

    long modificationTime();
}
