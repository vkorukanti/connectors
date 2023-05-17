package io.delta.kernel.client;

import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Provides file system related functionalities to Delta Kernel. Delta Kernel uses this client
 * whenever it needs to access the underlying file system where the Delta table is present.
 * Connector implementation of this interface can hide filesystem specific details from Delta
 * Kernel.
 */
public interface FileSystemClient
{
    /**
     * Given the path return an interator of files in given directory.
     *
     * @param directoryPath Fully qualified path to a directory.
     * @return Closeable iterator of files. It is the responsibility of the caller to close the
     *         iterator.
     * @throws FileNotFoundException if the given directory path is not found
     */
    CloseableIterator<FileStatus> listFiles(String directoryPath)
            throws FileNotFoundException;


    /**
     * Open the given file for reading and return an input stream
     *
     * @param filePath Fully qualified file path.
     * @return A {@link DataInputStream}. It is the responsibility of the caller to close the stream
     *         once done with it.
     * @throws IOException If an error occurs in opening the given file.
     */
    DataInputStream readFile(FileStatus filePath)
            throws IOException;
}
