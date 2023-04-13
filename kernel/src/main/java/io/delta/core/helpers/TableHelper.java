package io.delta.core.helpers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.InputFile;
import io.delta.core.data.Row;
import io.delta.core.fs.FileStatus;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public interface TableHelper extends Serializable
{
    CloseableIterator<FileStatus> listFiles(String path) throws FileNotFoundException;

    CloseableIterator<Row> readJsonFile(InputFile file, StructType readSchema) throws IOException;

    CloseableIterator<ColumnarBatch> readParquetFile(
            ConnectorReadContext context,
            InputFile path,
            StructType readSchema,
            Map<String, String> partitionValues) throws IOException;

    Row parseJson(String json, StructType schema);

    ScanHelper getScanHelper();
}
