package io.delta.kernel;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.utils.CloseableIterator;

/**
 * An object representing a scan of a Delta table.
 */
public interface Scan {
    /**
     * Get an iterator of data files to scan.
     *
     * @return data in {@link ColumnarBatch} batch format.
     *         Each row correspond to one scan file.
     */
    CloseableIterator<ColumnarBatch> getScanFiles();

    /**
     * Get the remaining filter the Delta Kernel can not guarantee the data returned by it
     * satisfy the filter. This filter is used by Delta Kernel to do data skipping whenever
     * possible.
     */
    Expression getRemainingFilter();

    /**
     * Get the scan state associate with the current scan.
     * This state is common to all survived files.
     * @return Scan state in {@link Row} format.
     */
    Row getScanState();
}
