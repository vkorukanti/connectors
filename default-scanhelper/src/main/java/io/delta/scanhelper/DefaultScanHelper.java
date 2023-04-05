
package io.delta.scanhelper;

import io.delta.standalone.core.DeltaScanHelper;
import io.delta.standalone.core.RowIndexFilter;
import io.delta.standalone.data.ColumnVector;
import io.delta.standalone.data.ColumnarRowBatch;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;

import java.io.DataInputStream;
import java.util.TimeZone;

import static java.util.Objects.requireNonNull;

public class DefaultScanHelper implements DeltaScanHelper {
    private final Configuration conf;

    public DefaultScanHelper(Configuration conf) {
        this.conf = requireNonNull(conf, "conf is null");
    }

    @Override
    public CloseableIterator<ColumnarRowBatch> readParquetFile(
            String filePath,
            StructType readSchema,
            TimeZone timeZone,
            RowIndexFilter deletionVector,
            Object opaqueConf)
    {
        return null;
    }

    @Override
    public CloseableIterator<RowRecord> readParquetFileRows(
            String filePath,
            StructType readSchema,
            TimeZone timeZone,
            RowIndexFilter deletionVector,
            Object opaqueConf)
    {
        ParquetRowRecordReader recordReader = new ParquetRowRecordReader(conf);
        return recordReader.read(filePath, readSchema);
    }

    @Override
    public DataInputStream readDeletionVectorFile(String filePath)
    {
        return null;
    }

    private static class DefaultColumnarBatch implements ColumnarRowBatch
    {
        private final StructType schema;

        DefaultColumnarBatch(StructType schema) {
            this.schema = requireNonNull(schema, "schema is null");
        }

        @Override
        public int getNumRows()
        {
            return 0;
        }

        @Override
        public StructType schema()
        {
            return schema;
        }

        @Override
        public ColumnVector getColumnVector(String columnName)
        {
            return null;
        }

        @Override
        public ColumnarRowBatch addColumnWithSingleValue(String columnName, DataType datatype, Object value)
        {
            return null;
        }

        @Override
        public void close()
        {

        }

        @Override
        public CloseableIterator<RowRecord> getRows()
        {
            return ColumnarRowBatch.super.getRows();
        }
    }
}
