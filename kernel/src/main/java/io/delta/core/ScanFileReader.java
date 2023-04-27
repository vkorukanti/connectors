package io.delta.core;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.InputFile;
import io.delta.core.data.Row;
import io.delta.core.fs.FileStatus;
import io.delta.core.helpers.ScanEnvironment;
import io.delta.core.helpers.TableHelper;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper methods provided by the Delta core to read data and
 * get the scan file status.
 */
public class ScanFileReader
{
    private ScanFileReader() { }

    /**
     * Get the {@link FileStatus} from given scan file row. {@link FileStatus} object allows the
     * connector to look at the partial metadata of the scan file.
     *
     * @param scanFileInfo Row representing one scan file.
     * @return a {@link FileStatus} object created from the given scan file row.
     */
    public static FileStatus getFileStatus(Row scanFileInfo) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Get the data from a scan file with the given connector's
     * {@link ScanEnvironment} and {@link TableHelper} implementations.
     *
     * @param scanFile an instance of {@link Row} representing a single scan file. This row is
     *                 from the {@link ColumnarBatch} returned by {@link Scan#getScanFiles()}
     * @param scanState Scan state returned by {@link Scan#getScanState()}
     * @param scanEnvironment Connector specific environment. For Delta core this is an opaque
     *                        object and its gets passed to given
     *                        {@link TableHelper#readParquetFile}
     * @param tableHelper Connector specific instance of {@link TableHelper}.
     * @param readSchema Select list of columns to read from the scan file. The column names are
     *                   logical column names exposed in table schema.
     * @return Data read from the scan file as an iterator of {@link ColumnarBatch}es. It is the
     *         responsibility of the caller to close the iterator.
     * @throws IOException when error occurs reading the data.
     */
    public static CloseableIterator<ColumnarBatch> readData(
            Row scanFile,
            Row scanState,
            ScanEnvironment scanEnvironment,
            TableHelper tableHelper,
            StructType readSchema) throws IOException {
        // TODO: should fetch this from the scan state
        ColumnMappingMode columnMappingMode = ColumnMappingMode.NONE;
        // TODO: should create this from the scan state and given readSchema.
        StructType physicalSchema = readSchema;
        // TODO: should get this from the scan file.
        Map<String, String> partitionValues = new HashMap<>();
        return tableHelper.readParquetFile(
                getFileStatus(scanFile),
                scanEnvironment,
                columnMappingMode,
                physicalSchema,
                partitionValues);
    }
}

