package io.delta.core.data;

import io.delta.core.types.DataType;
import io.delta.core.types.StructType;

public interface ColumnarBatch {

    StructType getSchema();

    ColumnVector getColumnVector(String columnName);

    void addConstantColumn(String topLevelColumnName, DataType dataType, Row value);

    void rename(String currentColumnName, String newColumnName);
}
