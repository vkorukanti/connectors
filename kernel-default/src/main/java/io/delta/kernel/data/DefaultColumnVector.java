package io.delta.kernel.data;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;
import org.apache.parquet.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * Wrapper around list of {@link Row}s to expose the rows as a columnar vector
 */
public class DefaultColumnVector implements ColumnVector
{
    private final DataType dataType;
    private final List<Row> rows;
    private final int columnOrdinal;

    public DefaultColumnVector(DataType dataType, List<Row> rows, int columnOrdinal)
    {
        this.dataType = dataType;
        this.rows = rows;
        this.columnOrdinal = columnOrdinal;
    }

    @Override
    public DataType getDataType()
    {
        return dataType;
    }

    @Override
    public int getSize()
    {
        return rows.size();
    }

    @Override
    public void close()
    {
        // nothing to close
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).isNullAt(columnOrdinal);
    }

    @Override
    public boolean getBoolean(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getBoolean(columnOrdinal);
    }

    @Override
    public byte getByte(int rowId)
    {
        assertValidRowId(rowId);
        return 0; // TODO
    }

    @Override
    public short getShort(int rowId)
    {
        assertValidRowId(rowId);
        return 0; // TODO
    }

    @Override
    public int getInt(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getInt(columnOrdinal);
    }

    @Override
    public long getLong(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getLong(columnOrdinal);
    }

    @Override
    public float getFloat(int rowId)
    {
        assertValidRowId(rowId);
        return 0; // TODO
    }

    @Override
    public double getDouble(int rowId)
    {
        assertValidRowId(rowId);
        return 0; // TODO
    }

    @Override
    public byte[] getBinary(int rowId)
    {
        assertValidRowId(rowId);
        return null; // TODO
    }

    @Override
    public <K, V> Map<K, V> getMap(int rowId)
    {
        return null;
    }

    @Override
    public Row getStruct(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getRecord(columnOrdinal);
    }

    @Override
    public <T> List<T> getArray(int rowId)
    {
        return null;
    }

    @Override
    public String getString(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getString(columnOrdinal);
    }

    private void assertValidRowId(int rowId) {
        Preconditions.checkArgument(
                rowId < rows.size(),
                "Invalid rowId: " + rowId + ", max allowed rowId is: " + (rows.size() - 1));
    }
}
