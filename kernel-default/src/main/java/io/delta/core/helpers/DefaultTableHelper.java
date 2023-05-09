package io.delta.core.helpers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.core.ColumnMappingMode;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.DefaultColumnarBatch;
import io.delta.core.data.JsonRow;
import io.delta.core.expressions.Expression;
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
    public CloseableIterator<FileStatus> listFiles(FileStatus file)
            throws FileNotFoundException
    {
        return new CloseableIterator<FileStatus>() {
            private final Iterator<org.apache.hadoop.fs.FileStatus> iter;

            {
                try {
                    iter = logStore.listFrom(new Path(file.getPath().toString()), hadoopConf);
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
            }

            @Override
            public void close() throws IOException { }
        };
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFile(FileStatus inputFile, StructType readSchema) throws FileNotFoundException {
        return new CloseableIterator<ColumnarBatch>() {
            private final io.delta.storage.CloseableIterator<String> iter;
            private ColumnarBatch nextBatch;

            {
                try {
                    iter = logStore.read(new Path(inputFile.getPath().toString()), hadoopConf);
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
                if (nextBatch == null) {
                    List<Row> rows = new ArrayList<>();
                    for (int i = 0; i < 1024 && iter.hasNext(); i++) {
                        // TODO: decide on the batch size
                        rows.add(parseJson(iter.next(), readSchema));
                    }
                    if (rows.isEmpty()) {
                        return false;
                    }
                    nextBatch = new DefaultColumnarBatch(readSchema, rows);
                }
                return true;
            }

            @Override
            public ColumnarBatch next() {
                // TODO: assert
                ColumnarBatch toReturn = nextBatch;
                nextBatch = null;
                return toReturn;
            }
        };
    }

    @Override
    public CloseableIterator<ColumnarBatch> readParquetFile(
            FileStatus file,
            ScanEnvironment scanEnvironment,
            ColumnMappingMode columnMappingMode,
            StructType readSchema,
            Map<String, String> partitionValues) throws IOException
    {
        StructType dataColumnSchema = removePartitionColumns(readSchema, partitionValues.keySet());
        DefaultScanEnvironment defaultScanTaskContext = (DefaultScanEnvironment) scanEnvironment;
        ParquetBatchReader batchReader = new ParquetBatchReader(hadoopConf);
        return batchReader.read(file.getPath().toString(), dataColumnSchema);
        // TODO: wrap the regular columnar batch iterator in a partition column generator
    }

    @Override
    public ExpressionEvaluator getExpressionEvaluator(StructType schema, Expression expression)
    {
        return new DefaultExpressionEvaluator(schema, expression);
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
