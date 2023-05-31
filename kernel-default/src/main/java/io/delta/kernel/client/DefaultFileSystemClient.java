package io.delta.kernel.client;

import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.storage.LocalLogStore;
import io.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

public class DefaultFileSystemClient
    implements FileSystemClient
{
    private final Configuration hadoopConf;
    private final LogStore logStore;

    public DefaultFileSystemClient(Configuration hadoopConf)
    {
        this.hadoopConf = hadoopConf;
        this.logStore = new LocalLogStore(hadoopConf);
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String filePath)
            throws FileNotFoundException
    {
        return new CloseableIterator<FileStatus>() {
            private final Iterator<org.apache.hadoop.fs.FileStatus> iter;

            {
                try {
                    iter = logStore.listFrom(new Path(filePath), hadoopConf);
                } catch (IOException ex) {
                    throw new RuntimeException("Could not resolve the FileSystem", ex);
                }
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public FileStatus next() {
                final org.apache.hadoop.fs.FileStatus impl = iter.next();
                return FileStatus.of(
                        impl.getPath().toString(),
                        impl.getLen(),
                        impl.getModificationTime());
            }

            @Override
            public void close() throws IOException { }
        };
    }
}
