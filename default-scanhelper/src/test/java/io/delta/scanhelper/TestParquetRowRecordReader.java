package io.delta.scanhelper;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.net.URL;

public class TestParquetRowRecordReader
{
    @Test
    public void testReaderSmallInts() throws Exception {
        URL url = this.getClass().getResource("ints-small.parquet");
        String filePath = url.getPath();
        StructType schema = new StructType().add("value", new IntegerType());

        ParquetRowRecordReader reader = new ParquetRowRecordReader(new Configuration());

        CloseableIterator<RowRecord> rows = reader.read(filePath, schema);
        while (rows.hasNext()) {
            RowRecord row = rows.next();
            System.out.printf("%s\n", row.toString());
        }
        rows.close();
    }

    @Test
    public void testReaderLargeInts() throws Exception {
        URL url = this.getClass().getResource("ints-large.parquet");
        String filePath = url.getPath();
        StructType schema = new StructType().add("value", new IntegerType());

        ParquetRowRecordReader reader = new ParquetRowRecordReader(new Configuration());

        CloseableIterator<RowRecord> rows = reader.read(filePath, schema);
        while (rows.hasNext()) {
            RowRecord row = rows.next();
            System.out.printf("%s\n", row.toString());
        }
        rows.close();
    }
}
