package io.delta.core;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.helpers.ConnectorReadContext;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

import java.io.IOException;

/**
 * Interface used by connectors to read data from a task created by Delta kernel.
 */
public interface ScanTask {

    /**
     * Read the data for all columns given in the <i>readSchema</i>.
     *
     * @param readContext Connector specific read context information which gets passed down all the
     *                    way to the {@link io.delta.core.helpers.TableHelper}. If the connector
     *                    has a custom implementation of the
     *                    {@link io.delta.core.helpers.TableHelper} then it can use the connector
     *                    specific context for additional optimizations.
     *
     * @param readSchema List of columns in the table for which data is requested. The columns
     *                   could be regular, partition or metadata columns. The names of the columns
     *                   should be logical names (TODO: should we even mention this?)
     * @return a
     *         {@link CloseableIterator} of {@link ColumnarBatch}s. The iterator should be closed to
     *         release any resources once done using it. It is the responsibility of
     *         the caller to close the iterator.
     * @throws IOException
     */
    CloseableIterator<ColumnarBatch> getData(
            ConnectorReadContext readContext,
            StructType readSchema
    ) throws IOException;

    /**
     * Read data for all columns. Returns .
     *
     * @param readContext Connector specific read context information which gets passed down all the
     *                    way to the {@link io.delta.core.helpers.TableHelper}. If the connector
     *                    has a custom implementation of the
     *                    {@link io.delta.core.helpers.TableHelper} then it can use the connector
     *                    specific context for additional optimizations.
     * @return a {@link CloseableIterator} of {@link ColumnarBatch}s.
     *         The iterator should be closed to release any resources once done using it. It is the
     *         responsibility of the caller to close the iterator
     * @throws IOException
     */
    CloseableIterator<ColumnarBatch> getData(
            ConnectorReadContext readContext
    ) throws IOException;
}
