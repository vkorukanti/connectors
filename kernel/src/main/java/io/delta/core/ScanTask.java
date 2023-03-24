package io.delta.core;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.utils.CloseableIterator;

public interface ScanTask {

    CloseableIterator<ColumnarBatch> getData();
}
