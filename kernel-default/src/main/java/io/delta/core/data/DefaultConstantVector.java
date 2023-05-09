package io.delta.core.data;

import io.delta.core.types.DataType;

import java.util.List;
import java.util.Map;

public class DefaultConstantVector implements ColumnVector
{
    private final DataType dataType;
    private final int numRows;
    private final Object value;

    public DefaultConstantVector(DataType dataType, int numRows, Object value)
    {
        // TODO: Validate datatype and value object type
        this.dataType = dataType;
        this.numRows = numRows;
        this.value = value;
    }

    @Override
    public DataType getDataType()
    {
        return dataType;
    }

    @Override
    public int getSize()
    {
        return numRows;
    }

    @Override
    public void close()
    {
        // nothing to close
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return value == null;
    }

    @Override
    public boolean getBoolean(int rowId)
    {
        return (boolean) value;
    }

    @Override
    public byte getByte(int rowId)
    {
        return (byte) value;
    }

    @Override
    public short getShort(int rowId)
    {
        return (short) value;
    }

    @Override
    public int getInt(int rowId)
    {
        return (int) value;
    }

    @Override
    public long getLong(int rowId)
    {
        return (long) value;
    }

    @Override
    public float getFloat(int rowId)
    {
        return (float) value;
    }

    @Override
    public double getDouble(int rowId)
    {
        return (double) value;
    }

    @Override
    public byte[] getBinary(int rowId)
    {
        return (byte[]) value;
    }

    @Override
    public String getString(int rowId)
    {
        return (String) value;
    }

    @Override
    public <K, V> Map<K, V> getMap(int rowId)
    {
        return (Map<K, V>) value;
    }

    @Override
    public Row getStruct(int rowId)
    {
        return (Row) value;
    }

    @Override
    public <T> List<T> getArray(int rowId)
    {
        return (List<T>) value;
    }

}
