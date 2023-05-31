package io.delta.kernel.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.JsonRow;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;
import io.delta.storage.LocalLogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DefaultJsonHandler
    implements JsonHandler
{
    private final ObjectMapper objectMapper;
    private final Configuration hadoopConf;
    private final LocalLogStore logStore;

    public DefaultJsonHandler(Configuration hadoopConf)
    {
        this.objectMapper = new ObjectMapper();
        this.hadoopConf = hadoopConf;
        this.logStore = new LocalLogStore(hadoopConf);
    }

    @Override
    public CloseableIterator<FileReadContext> contextualizeFileReads(
            CloseableIterator<Row> fileIter,
            Expression predicate)
    {
        return new CloseableIterator<FileReadContext>() {
            @Override
            public void close()
                    throws IOException
            {
                fileIter.close();
            }

            @Override
            public boolean hasNext()
            {
                return fileIter.hasNext();
            }

            @Override
            public FileReadContext next()
            {
                return () -> fileIter.next();
            }
        };
    }

    @Override
    public ColumnarBatch parseJson(ColumnVector jsonStringVector, StructType outputSchema)
    {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < jsonStringVector.getSize(); i++) {
            rows.add(parseJson(jsonStringVector.getString(i), outputSchema));
        }
        return new DefaultColumnarBatch(outputSchema, rows);
    }

    @Override
    public CloseableIterator<FileDataReadResult> readJsonFiles(
            CloseableIterator<FileReadContext> fileIter,
            StructType physicalSchema) throws IOException
    {
        return new CloseableIterator<FileDataReadResult>() {
            private FileReadContext currentFile;
            private io.delta.storage.CloseableIterator<String> currentFileReader;

            @Override
            public void close()
                    throws IOException
            {
                if (currentFileReader != null) {
                    currentFileReader.close();
                }

                fileIter.close();
                // TODO: implement safe close of multiple closeables.
            }

            @Override
            public boolean hasNext()
            {
                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                if (currentFileReader == null || !currentFileReader.hasNext()) {
                    if (fileIter.hasNext()) {
                        try {
                            currentFile = fileIter.next();
                            FileStatus fileStatus =
                                    Utils.getFileStatus(currentFile.getScanFileRow());
                            currentFileReader = logStore.read(new Path(fileStatus.getPath()), hadoopConf);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        return false;
                    }
                }

                return currentFileReader.hasNext();
            }

            @Override
            public FileDataReadResult next()
            {
                List<Row> rows = new ArrayList<>();
                for (int i = 0; i < 1024 && currentFileReader.hasNext(); i++) {
                    // TODO: decide on the batch size
                    rows.add(parseJson(currentFileReader.next(), physicalSchema));
                }
                ColumnarBatch nextBatch = new DefaultColumnarBatch(physicalSchema, rows);

                return new FileDataReadResult() {
                    @Override
                    public ColumnarBatch getData()
                    {
                        return nextBatch;
                    }

                    @Override
                    public Row getScanFileRow()
                    {
                        return currentFile.getScanFileRow();
                    }
                };
            }
        };
    }

    private Row parseJson(String json, StructType readSchema)
    {
        try {
            final JsonNode jsonNode = objectMapper.readTree(json);
            return new JsonRow((ObjectNode) jsonNode, readSchema);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
        }
    }
}
