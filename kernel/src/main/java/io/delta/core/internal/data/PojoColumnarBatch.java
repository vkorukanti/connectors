package io.delta.core.internal.data;

import io.delta.core.data.ColumnVector;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.types.DataType;
import io.delta.core.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Exposes a given a list of POJO objects and their schema as a {@link ColumnarBatch}.
 */
public class PojoColumnarBatch<POJO_TYPE>
    implements ColumnarBatch
{
    private final List<POJO_TYPE> pojoObjects;
    private final StructType schema;
    private final Map<Integer, Function<POJO_TYPE, Object>> ordinalToAccessor;
    private final Map<Integer, String> ordinalToColName;

    public PojoColumnarBatch(
            List<POJO_TYPE> pojoObjects,
            StructType schema,
            Map<Integer, Function<POJO_TYPE, Object>> ordinalToAccessor,
            Map<Integer, String> ordinalToColName)
    {
        this.pojoObjects = requireNonNull(pojoObjects, "pojoObjects is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.ordinalToAccessor = requireNonNull(ordinalToAccessor, "ordinalToAccessor is null");
        this.ordinalToColName = requireNonNull(ordinalToColName, "ordinalToColName is null");
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        return new ColumnVector() {
            @Override
            public DataType getDataType()
            {
                // TODO: null safe
                return getSchema().get(ordinalToColName.get(ordinal)).getDataType();
            }

            @Override
            public int getSize()
            {
                return pojoObjects.size();
            }

            @Override
            public void close() { /* Nothing to close */ }

            @Override
            public boolean isNullAt(int rowId)
            {
                return getValue(rowId) == null;
            }

            @Override
            public boolean getBoolean(int rowId)
            {
                // TODO: safe typecast
                return (boolean) getValue(rowId);
            }

            @Override
            public byte getByte(int rowId)
            {
                // TODO: safe typecast
                return (Byte) getValue(rowId);
            }

            @Override
            public short getShort(int rowId)
            {
                // TODO: safe typecast
                return (short) getValue(rowId);
            }

            @Override
            public int getInt(int rowId)
            {
                // TODO: safe typecast
                return (int) getValue(rowId);
            }

            @Override
            public long getLong(int rowId)
            {
                // TODO: safe typecast
                return (long) getValue(rowId);
            }

            @Override
            public float getFloat(int rowId)
            {
                // TODO: safe typecast
                return (float) getValue(rowId);
            }

            @Override
            public double getDouble(int rowId)
            {
                // TODO: safe typecast
                return (double) getValue(rowId);
            }

            @Override
            public byte[] getBinary(int rowId)
            {
                // TODO: safe typecast
                return (byte[]) getValue(rowId);
            }

            @Override
            public String getString(int rowId)
            {
                // TODO: safe typecast
                return (String) getValue(rowId);
            }

            private Object getValue(int rowId)
            {
                return ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            @Override
            public Object getMap(int rowId)
            {
                return ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            @Override
            public Object getStruct(int rowId)
            {
                return ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            @Override
            public Object getArray(int rowId)
            {
                return ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }
        };
    }

    @Override
    public int getSize()
    {
        return pojoObjects.size();
    }
}
