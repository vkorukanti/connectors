package io.delta.core.helpers;

import java.io.FileNotFoundException;

import io.delta.core.data.Row;
import io.delta.core.expressions.Expression;
import io.delta.core.fs.FileStatus;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public interface TableHelper {

    CloseableIterator<FileStatus> listFiles(String path) throws FileNotFoundException;

    // TODO: we should update LogStore.java :: read to throw a FileNotFoundException
    CloseableIterator<Row> readJsonFile(String path, StructType readSchema) throws FileNotFoundException;

    /** Uses the readSchema for partition pruning. */
    CloseableIterator<Row> readParquetFile(String path, StructType readSchema) throws FileNotFoundException;

    /** Uses the readSchema for partition pruning and the skippingFilter for data filtering. */
    CloseableIterator<Row> readParquetFile(
        String path,
        StructType readSchema,
        Expression skippingFilter);

    Row parseStats(String statsJson, StructType statsSchema);

    StructType parseSchema(String schemaJson);

    ScanHelper getScanHelper();
}
