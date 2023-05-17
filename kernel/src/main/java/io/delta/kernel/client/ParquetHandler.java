package io.delta.kernel.client;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.parquet.ParquetFooter;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import java.io.IOException;
import java.util.Map;

/**
 * Provides Parquet file related functionalities to Delta Kernel. Connectors can leverage this
 * interface to provide their own custom implementation of Parquet data file functionalities to
 * Delta Kernel.
 */
public interface ParquetHandler
{
    /**
     * Read the given Parquet file path and return the footer information.
     * @param filePath Fully qualified Parquet file path.
     * @return A {@link ParquetFooter} populated from the Parquet file.
     */
    ParquetFooter readParquetFooter(String filePath);

    /**
     * Associate a connector specific {@link FileReadContext} for each given {@link FileStatus}.
     * The Delta Kernel uses the returned pair of {@link FileStatus} and {@link FileReadContext}
     * to request data from the connector. Delta Kernel doesn't interpret the
     * {@link FileReadContext}. It just passes it back to the connector for interpretation.
     * For example the connector can attach split information in its own implementation of
     * {@link FileReadContext} or attach any predicates.
     *
     * @param fileIter Iterator of {@link FileStatus}
     * @param predicate Predicate to use prune file list. This is optional for the connector to use.
     *                  Delta Kernel doesn't require the connector filtering the data by this
     *                  predicate.
     * @return Itearator of {@link FileStatus} and {@link FileReadContext} typles to read data from.
     */
    CloseableIterator<Tuple2<FileStatus, FileReadContext>> contextualizeFiles(
            CloseableIterator<FileStatus> fileIter,
            Expression predicate);

    /**
     * Read the Parquet format file at given location and return the data.
     *
     * @param fileIter Iterator of ({@link FileStatus}, {@link FileReadContext}) objects to read
     *                 data from.
     * @param physicalSchema Select list of columns to read from the Parquet file.
     * @return an iterator of data in columnar format. It is the responsibility of the caller to
     *         close the iterator. The data returned is in the same as the order of files given in
     *         <i>fileIter</i>.
     * @throws IOException if an error occurs during the read.
     */
    CloseableIterator<ParquetDataReadResult> readParquetFiles(
            CloseableIterator<Tuple2<FileStatus, FileReadContext>> fileIter,
            StructType physicalSchema,
            Map<String, String> partitionValues) throws IOException;

    /**
     * Data read from a Parquet file with {@link FileStatus} attached. The {@link FileStatus} object
     * should be same as the one given by the Delta Kernel.
     */
    interface ParquetDataReadResult
    {
        /**
         * Get the data read from the Parquet file.
         * @return Data in {@link ColumnarBatch} format.
         */
        ColumnarBatch getData();

        /**
         * Get the {@link FileStatus} of the file from which the data is read. It should be the
         * same object the Delta Kernel has passed to the connector to read the data from.
         * @return {@link FileStatus} object of the data file.
         */
        FileStatus getFileStatus();
    }
}
