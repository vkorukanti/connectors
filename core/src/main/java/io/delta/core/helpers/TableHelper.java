package io.delta.core.helpers;

import java.io.FileNotFoundException;

import io.delta.core.data.Row;
import io.delta.core.expressions.Expression;
import io.delta.core.fs.FileStatus;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public interface TableHelper {

    CloseableIterator<FileStatus> listFiles(String path) throws FileNotFoundException;

    CloseableIterator<Row> readJsonFile(String path, StructType readSchema);

    /** Uses the readSchema for partition pruning. */
    CloseableIterator<Row> readParquetFile(String path, StructType readSchema);

    /** Uses the readSchema for partition pruning and the skippingFilter for data filtering. */
    CloseableIterator<Row> readParquetFile(
        String path,
        StructType readSchema,
        Expression skippingFilter);

    Row parseStats(String statsJson);

    ScanHelper getScanHelper();
}
