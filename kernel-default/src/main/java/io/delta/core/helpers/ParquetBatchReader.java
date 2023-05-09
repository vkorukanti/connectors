package io.delta.core.helpers;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.DefaultColumnarBatch;
import io.delta.core.data.Row;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ParquetBatchReader
{
    private final Configuration configuration;

    public ParquetBatchReader(Configuration configuration)
    {
        this.configuration = requireNonNull(configuration, "configuration is null");
    }

    public CloseableIterator<ColumnarBatch> read(String path, StructType schema) {
        ParquetRecordReader<Row> reader =
                new ParquetRecordReader<>(
                        new ParquetRowReader.RowReadSupport(schema),
                        FilterCompat.NOOP);

        Path filePath = new Path(path);
        try {
            FileSystem fs = filePath.getFileSystem(configuration);
            FileStatus fileStatus = fs.getFileStatus(filePath);
            reader.initialize(
                    new FileSplit(filePath, 0, fileStatus.getLen(), new String[0]),
                    configuration,
                    Reporter.NULL
            );
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        return new CloseableIterator<ColumnarBatch>() {
            @Override
            public void close()
                    throws IOException
            {
                reader.close();
            }

            @Override
            public boolean hasNext()
            {
                try {
                    return reader.nextKeyValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public ColumnarBatch next()
            {
                try {
                    List<Row> rows = new ArrayList<>();
                    for (int i = 0; i < 1024; i++) {
                        rows.add(reader.getCurrentValue());
                        if (!hasNext()) {
                            break;
                        }
                    }
                    return new DefaultColumnarBatch(schema, rows);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
