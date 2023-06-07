package io.delta.kernel.client;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultColumnarBatch;
import io.delta.kernel.data.ParquetRowRecord;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.api.ReadSupport;

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
        int rowIndexColIdx = schema.indexOf(StructField.ROW_INDEX_COLUMN_NAME);
        ParquetRecordReader<ParquetRowRecord> reader;
        if (rowIndexColIdx >= 0 && schema.at(rowIndexColIdx).isMetadataColumn()) {
            reader = new ParquetRecordReaderWithRowIndexes(
                    new ParquetRowReader.RowReadSupport(schema),
                    FilterCompat.NOOP,
                    rowIndexColIdx);
        } else {
            reader = new ParquetRecordReader<>(
                    new ParquetRowReader.RowReadSupport(schema),
                    FilterCompat.NOOP);
        }

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

    /**
     * A {@link ParquetRecordReader<ParquetRowRecord>} that populates a
     * {@link StructField#ROW_INDEX_COLUMN_NAME} column with the current row index value from the
     * parquet-mr {@link ParquetRecordReader#getCurrentRowIndex()} API when calling
     * {@code getCurrentValue()}
     */
    class ParquetRecordReaderWithRowIndexes extends ParquetRecordReader<ParquetRowRecord> {

        private final int rowIndexColIdx;

        /**
         * @param rowIndexColIdx the index of the row_index column to populate
         */
        public ParquetRecordReaderWithRowIndexes(
                ReadSupport<ParquetRowRecord> readSupport,
                FilterCompat.Filter filter,
                int rowIndexColIdx) {
            super(readSupport, filter);
            this.rowIndexColIdx = rowIndexColIdx;
        }

        @Override
        public ParquetRowRecord getCurrentValue() throws IOException, InterruptedException {
            ParquetRowRecord row = super.getCurrentValue();
            row.setLong(rowIndexColIdx, super.getCurrentRowIndex());
            return row;
        }
    }
}
