package io.delta.core;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.Row;
import io.delta.core.helpers.ConnectorReadContext;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

import java.io.IOException;

public interface ScanState
{
    CloseableIterator<ColumnarBatch> getData(
            Row scanState,
            Row fileInfo,
            StructType readSchema,
            ConnectorReadContext connectorReadContext
    ) throws IOException;
}
