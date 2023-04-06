package io.delta.core;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

import java.io.IOException;

class Context
{

}

public interface ScanTask {
    CloseableIterator<ColumnarBatch> getData(StructType readSchema, Context context) throws IOException;
}
