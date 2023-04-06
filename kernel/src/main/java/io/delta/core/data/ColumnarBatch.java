package io.delta.core.data;

import io.delta.core.types.DataType;
import io.delta.core.types.StructType;

public interface ColumnarBatch {
    ColumnVector getColumnVector(String ordinal);
}
