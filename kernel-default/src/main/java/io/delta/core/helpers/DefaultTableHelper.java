package io.delta.core.helpers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.core.data.JsonRow;
import org.apache.hadoop.conf.Configuration;

import io.delta.core.data.Row;
import io.delta.core.expressions.Expression;
import io.delta.core.fs.FileStatus;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;
import io.delta.storage.LocalLogStore;
import io.delta.storage.LogStore;
import org.apache.hadoop.fs.Path;

public class DefaultTableHelper implements TableHelper {

    private final Configuration hadoopConf;
    private final LogStore logStore;
    private final ObjectMapper objectMapper;

    public DefaultTableHelper() {
        this.hadoopConf = new Configuration();
        this.logStore = new LocalLogStore(hadoopConf);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public CloseableIterator<FileStatus> listFiles(String path) {
        System.out.println("Scott > DefaultTableHelper > listFiles :: path " + path);
        return new CloseableIterator<FileStatus>() {
            private final Iterator<org.apache.hadoop.fs.FileStatus> iter;

            {
                try {
                    iter = logStore.listFrom(new Path(path), hadoopConf);
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
                return new FileStatus() {
                    final org.apache.hadoop.fs.FileStatus impl = iter.next();
                    {
                        System.out.println("listFiles > next :: " + impl.getPath());
                    }

                    @Override
                    public String path() {
                        return impl.getPath().toString();
                    }

                    @Override
                    public long length() {
                        return impl.getLen();
                    }

                    @Override
                    public long modificationTime() {
                        return impl.getModificationTime();
                    }
                };
            }

            @Override
            public void close() throws IOException { }
        };
    }

    @Override
    public CloseableIterator<Row> readJsonFile(String path, StructType readSchema) throws FileNotFoundException {
        return new CloseableIterator<Row>() {
            private final io.delta.storage.CloseableIterator<String> iter;

            {
                try {
                    iter = logStore.read(new Path(path), hadoopConf);
                } catch (IOException ex) {
                    if (ex instanceof FileNotFoundException) {
                        throw (FileNotFoundException) ex;
                    }

                    throw new RuntimeException("Could not resolve the FileSystem", ex);
                }
            }

            @Override
            public void close() throws IOException {
                iter.close();
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Row next() {
                final String json = iter.next();
                try {
                    final JsonNode jsonNode = objectMapper.readTree(json);
                    return new JsonRow((ObjectNode) jsonNode, readSchema);
                } catch (JsonProcessingException ex) {
                    throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
                }
            }
        };
    }

    @Override
    public CloseableIterator<Row> readParquetFile(String path, StructType readSchema) {
        return null;
    }

    @Override
    public CloseableIterator<Row> readParquetFile(String path, StructType readSchema, Expression skippingFilter) {
        return null;
    }

    @Override
    public Row parseStats(String statsJson) {
        return null;
    }

    @Override
    public ScanHelper getScanHelper() {
        return null;
    }
}
