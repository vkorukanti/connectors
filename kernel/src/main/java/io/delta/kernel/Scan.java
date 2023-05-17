package io.delta.kernel;

import io.delta.kernel.client.TableClient;
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
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return data in {@link ColumnarBatch} batch format.
     *         Each row correspond to one scan file.
     */
    CloseableIterator<ColumnarBatch> getScanFiles(TableClient tableClient);

    /**
     * Get the remaining predicate the Delta Kernel can not guarantee the data returned by it
     * satisfies the predicate. This filter is used by Delta Kernel to do data skipping whenever
     * possible.
     * @return Remaining predicate as {@link Expression}.
     */
    Expression getRemainingPredicate();

    /**
     * Get the scan state associate with the current scan. This state is common to all survived
     * files.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return Scan state in {@link Row} format.
     */
    Row getScanState(TableClient tableClient);
}
