package io.delta.core.internal;

import io.delta.core.data.ColumnVector;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.Row;
import io.delta.core.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Row abstraction around a columnar batch and a particular row within the columnar batch.
 */
public class ColumnarBatchRow implements Row
{
    private final ColumnarBatch columnarBatch;
    private final int rowId;

    public ColumnarBatchRow(ColumnarBatch columnarBatch, int rowId)
    {
        this.columnarBatch = Objects.requireNonNull(columnarBatch, "columnarBatch is null");
        this.rowId = rowId;
    }

    @Override
    public StructType getSchema()
    {
        return columnarBatch.getSchema();
    }

    @Override
    public boolean isNullAt(int ordinal)
    {
        return columnVector(ordinal).isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int ordinal)
    {
        return columnVector(ordinal).getBoolean(rowId);
    }

    @Override
    public int getInt(int ordinal)
    {
        return columnVector(ordinal).getInt(rowId);
    }

    @Override
    public long getLong(int ordinal)
    {
        return columnVector(ordinal).getLong(rowId);
    }

    @Override
    public String getString(int ordinal)
    {
        return columnVector(ordinal).getString(rowId);
    }

    @Override
    public Row getRecord(int ordinal)
    {
        return columnVector(ordinal).getStruct(rowId);
    }

    @Override
    public <T> List<T> getList(int ordinal)
    {
        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal)
    {
        return null;
    }

    private ColumnVector columnVector(int ordinal) {
        return columnarBatch.getColumnVector(ordinal);
    }
}
