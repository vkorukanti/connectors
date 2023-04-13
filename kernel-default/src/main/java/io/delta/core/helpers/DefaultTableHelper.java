package io.delta.core.helpers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.InputFile;
import io.delta.core.data.JsonRow;
import io.delta.core.types.StructField;
import org.apache.hadoop.conf.Configuration;

import io.delta.core.data.Row;
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
                final org.apache.hadoop.fs.FileStatus impl = iter.next();
                return new FileStatus(impl.getPath().toString(), impl.getLen(), impl.getModificationTime());
//                return new FileStatus() {
//                    final org.apache.hadoop.fs.FileStatus impl = iter.next();
//
//                    @Override
//                    public String pathStr() {
//                        return impl.getPath().toString();
//                    }
//
//                    @Override
//                    public long length() {
//                        return impl.getLen();
//                    }
//
//                    @Override
//                    public long modificationTime() {
//                        return impl.getModificationTime();
//                    }
//                };
            }

            @Override
            public void close() throws IOException { }
        };
    }

    @Override
    public CloseableIterator<Row> readJsonFile(InputFile inputFile, StructType readSchema) throws FileNotFoundException {
        return new CloseableIterator<Row>() {
            private final io.delta.storage.CloseableIterator<String> iter;

            {
                try {
                    iter = logStore.read(new Path(inputFile.getPath()), hadoopConf);
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
                return parseJson(iter.next(), readSchema);
            }
        };
    }

    @Override
    public CloseableIterator<ColumnarBatch> readParquetFile(
            ConnectorReadContext readContext,
            InputFile file,
            StructType readSchema,
            Map<String, String> partitionValues) throws IOException
    {
        StructType dataColumnSchema = removePartitionColumns(readSchema, partitionValues.keySet());
        DefaultConnectorReadContext defaultScanTaskContext = (DefaultConnectorReadContext) readContext;
        ParquetBatchReader batchReader = new ParquetBatchReader(hadoopConf);
        return batchReader.read(file.getPath(), dataColumnSchema);
        // TODO: wrap the regular columnar batch iterator in a partition column generator
    }

    @Override
    public Row parseJson(String json, StructType readSchema) {
        try {
            final JsonNode jsonNode = objectMapper.readTree(json);
            return new JsonRow((ObjectNode) jsonNode, readSchema);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
        }
    }

    @Override
    public ScanHelper getScanHelper() {
        return null;
    }

    private static StructType removePartitionColumns(
            StructType readSchema,
            Set<String> partitionColumns) {
        StructType dataColumnSchema = new StructType();

        for (StructField field : readSchema.fields()) {
            if (!partitionColumns.contains(field.getName())) {
                dataColumnSchema = dataColumnSchema.add(field);
            }
        }
        return dataColumnSchema;
    }
}
