package io.delta.kernel.helpers;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import io.delta.kernel.ColumnMappingMode;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Helper class which provides information needed by the Delta Core to load the Delta metadata
 */
public interface TableHelper
{
    /**
     * Given the path return an interator of files from the file
     *
     * @param file Directory path
     * @return Closeable iterator of files. It is the responsibility of the caller to close the
     *         iterator.
     * @throws FileNotFoundException if the given file is not found
     */
    CloseableIterator<FileStatus> listFiles(FileStatus file)
            throws FileNotFoundException;

    /**
     * Read the JSON format file at given location return the data.
     *
     * @param file {@link FileStatus} of the scan file.
     * @param readSchema Select list of columns to read from the JSON file.
     * @return an iterator of data in columnar format. It is the responsibility of the caller to
     *         close the iterator.
     * @throws IOException if an error occurs during the read.
     */
    CloseableIterator<ColumnarBatch> readJsonFile(FileStatus file, StructType readSchema)
            throws IOException;

    /**
     * Read the Parquet format file at given location and return the data.
     *
     * @param file {@link FileStatus} of the scan file.
     * @param scanFileContext Optional connector specific scan file context.
     * @param columnMappingMode How to look up the given columns in <i>readSchema</i> in Parquet
     *                          file?
     * @param readSchema Select list of columns to read from the Parquet file.
     * @param partitionValues Key value map of partition column and partition value pairs.
     * @return an iterator of data in columnar format. It is the responsibility of the caller to
     *         close the iterator.
     * @throws IOException if an error occurs during the read.
     */
    CloseableIterator<ColumnarBatch> readParquetFile(
            FileStatus file,
            Optional<ScanFileContext> scanFileContext,
            ColumnMappingMode columnMappingMode,
            StructType readSchema,
            Map<String, String> partitionValues) throws IOException;

    /**
     * Open the given file for reading and return an input stream
     * @param file to be opened
     * @return {@link DataInputStream}. It is the responsibility of the caller to close the stream
     *         once done with it.
     * @throws IOException If an error occurs in opening the given file.
     */
    DataInputStream readFile(FileStatus file) throws IOException;

    /**
     * Return an expression evaluator for given schema and expression. The returned evaluator takes
     * a {@link ColumnarBatch} as input and returns the expression output as a {@link ColumnVector}.
     *
     * @param schema expected schema of the input
     *               {@link ColumnarBatch} to returned evaluator
     * @param expression to evaluate.
     * @return {@link ExpressionEvaluator}
     */
    ExpressionEvaluator getExpressionEvaluator(StructType schema, Expression expression);

    /**
     * Helper method to parse the JSON given as string and return the result as {@link Row}.
     *
     * @param json JSON in string format
     * @param schema Subset of columns to read from the JSON
     * @return
     */
    Row parseJson(String json, StructType schema);
}
