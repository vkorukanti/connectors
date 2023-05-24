package io.delta.kernel.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultColumnarBatch;
import io.delta.kernel.data.JsonRow;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
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
    public Row parseJson(String json, StructType schema)
    {
        try {
            final JsonNode jsonNode = objectMapper.readTree(json);
            return new JsonRow((ObjectNode) jsonNode, schema);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
        }
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFile(FileStatus fileStatus, StructType schema)
    {
        return new CloseableIterator<ColumnarBatch>() {
            private final io.delta.storage.CloseableIterator<String> iter;
            private ColumnarBatch nextBatch;

            {
                try {
                    iter = logStore.read(new Path(fileStatus.getPath()), hadoopConf);
                } catch (IOException ex) {
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
                        rows.add(parseJson(iter.next(), schema));
                    }
                    if (rows.isEmpty()) {
                        return false;
                    }
                    nextBatch = new DefaultColumnarBatch(schema, rows);
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
}
