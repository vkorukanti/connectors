package io.delta.kernel;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * Helper methods provided by the Delta core to read data from a scan file entry and get metadata
 * about the scan file entry.
 */
public class ScanFile
{
    private ScanFile() { }

    /**
     * Get the {@link FileStatus} from given scan file row. {@link FileStatus} object allows the
     * connector to look at the partial metadata of the scan file.
     *
     * @param scanFileInfo Row representing one scan file.
     * @return a {@link FileStatus} object created from the given scan file row.
     */
    public static FileStatus getFileStatus(Row scanFileInfo) {
        String path = scanFileInfo.getString(0);
        Long size = scanFileInfo.getLong(2);
        boolean hasDeletionVector = scanFileInfo.isNullAt(5);

        return FileStatus.of(path, size, hasDeletionVector);
    }

    /**
     * Get the data from a given scan files with using the connector provider {@link TableClient}.
     *
     * @param tableClient Connector provided {@link TableClient} implementation.
     * @param scanState Scan state returned by {@link Scan#getScanState(TableClient)}
     * @param scanFileRows an iterator of {@link Row}s. Each {@link Row} represents one scan file
     *                     from the {@link ColumnarBatch} returned by
     *                     {@link Scan#getScanFiles(TableClient)}
     * @param predicate An optional predicate that can be used for data skipping while reading the
     *                  scan files.
     * @return Data read from the scan file as an iterator of {@link ColumnarBatch}es and
     *         {@link ColumnVector} pairs. The {@link ColumnVector} represents a selection vector
     *         of type boolean that has the same size as the data {@link ColumnarBatch}. A value
     *         of true at a given index indicates the row with the same index from the data
     *         {@link ColumnarBatch} should be considered and a value of false at a given index
     *         indicates the row with the same index in data {@link ColumnarBatch} should be
     *         ignored. It is the responsibility of the caller to close the iterator.
     *
     * @throws IOException when error occurs reading the data.
     */
    public static CloseableIterator<Tuple2<ColumnarBatch, ColumnVector>>
            readData(
                    TableClient tableClient,
                    Row scanState,
                    Iterator<Row> scanFileRows,
                    Optional<Expression> predicate) throws IOException {
//        // TODO: should fetch this from the scan state
//        ColumnMappingMode columnMappingMode = ColumnMappingMode.NONE;
//        // TODO: should create this from the scan state and given readSchema.
//        StructType physicalSchema = readSchema;
//        // TODO: should get this from the scan file.
//        Map<String, String> partitionValues = new HashMap<>();
//        String tableDataPath = scanState.getString(5);
//        return tableHelper.readParquetFile(
//                withAbsolutePath(getFileStatus(scanFile), tableDataPath),
//                scanFileContext,
//                columnMappingMode,
//                physicalSchema,
//                partitionValues);
        throw new UnsupportedOperationException("Not yet implemented");
//
//        Pseudo code from discussion with RJ.
//        FileStatus fileStatus = KernelUtil.getFileStatus(scanFileRow)
//        StructType logicalSchema = KernelUtil.getLogicalSchema(scanStateRow)
//        // also adds the row_index and row_id columns based on the
//// metadata, protocol
//        StructType physicalSchema = KernelUtil.getPhysicalSchema(scanStateRow)
//
//
//        Expression predicateOnPhysicalSchema =
//                KernelUtil.physicalize(scanStateRow, predicateOnLogicalSchema)
//
//
//// TODO: we have chosen to have the same physical schema including row_index for
// all files even if they dont have DVs and does not need row_index based filtering
//
//        Iterator<FileStatus, FileReadContext> filesWithContextIter =
//                parquetReader.contextualizeFiles(
//                        Iterator of (scanFilesRows -> FileStatus),
//                        predicateOnLogicalSchema
//                )
//
//        Iterator<ColumnarBatchResult> dataIter = parquetReader.readParquetFiles(
//                filesWithContextIter,
//                physicalSchema)
//
//        foreach(ColumnarBatchResult data : dataIter) {
//            FileStatus file = data.getFile()
//
//            // Read DV and generate the RowIndex filter
//
//        }
    }
}

