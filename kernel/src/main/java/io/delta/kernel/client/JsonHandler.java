package io.delta.kernel.client;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Provides JSON handling functionality to Delta Kernel. Delta Kernel can use this client to
 * parse JSON strings into {@link io.delta.kernel.data.Row}. Connectors can leverage
 * this interface to provide their best implementation of the JSON parsing capability to
 * Delta Kernel.
 */
public interface JsonHandler
{
    /**
     * Parse the given <i>json</i> string and return the requested fields in given <i>schema</i>
     * as a {@link Row}.
     *
     * @param json Valid JSON object in string format.
     * @param schema Schema of the data to return from the parse JSON. If any requested fields
     *               are missing in the JSON string, a <i>null</i> is returned for that particular
     *               field in returned {@link Row}. The type for each given field is expected to
     *               match the type in JSON.
     * @return
     */
    Row parseJson(String json, StructType schema);

    // TODO(Verify with TD): Should this be similar to the Parquet version to allow multiple file
    //  reading?
    /**
     * Read the given JSON file. Each JSON object in the files is on one line. The implementation
     * can use this property to break the file into rows.
     * @param fileStatus JSON file to read.
     * @param schema List of columns to read from the JSON file. If a column is not present in
     *               the JSON object, a null is returned for the column value.
     * @return JSON data mentioned in the <i>schema</i> as {@link ColumnarBatch} batch.
     */
    CloseableIterator<ColumnarBatch> readJsonFile(
            FileStatus fileStatus,
            StructType schema);
}
