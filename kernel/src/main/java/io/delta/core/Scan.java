package io.delta.core;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.Row;
import io.delta.core.utils.CloseableIterator;

public interface Scan {
    /**
     * Get an iterator of data files to scan.
     *
     * @return data in {@link ColumnarBatch} batch format.
     *         Each row correspond to one scan file.
     */
    CloseableIterator<ColumnarBatch> getScanFiles();

    /**
     * Get the scan state associate with the current scan.
     * This state is common to all survived files.
     * @return Scan state in {@link Row} format.
     */
    Row getScanState();
}
