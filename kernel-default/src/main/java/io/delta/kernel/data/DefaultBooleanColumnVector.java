package io.delta.kernel.data;

import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;

import java.util.List;
import java.util.Map;

public class DefaultBooleanColumnVector
    implements ColumnVector
{
    private final List<Boolean> values;

    public DefaultBooleanColumnVector(List<Boolean> values) {
        this.values = values; // TODO: validation and use ImmutableList
    }

    @Override
    public DataType getDataType()
    {
        return BooleanType.INSTANCE;
    }

    @Override
    public int getSize()
    {
        return values.size();
    }

    @Override
    public void close() { }

    @Override
    public boolean isNullAt(int rowId)
    {
        return values.get(rowId) == null;
    }

    @Override
    public boolean getBoolean(int rowId)
    {
        return values.get(rowId);
    }

    @Override
    public byte getByte(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public short getShort(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public int getInt(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public long getLong(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public float getFloat(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public double getDouble(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public byte[] getBinary(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public String getString(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public <K, V> Map<K, V> getMap(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public Row getStruct(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public <T> List<T> getArray(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }
}
