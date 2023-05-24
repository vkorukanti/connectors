package io.delta.kernel.internal.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.expressions.Column;

/**
 * The type of Row that will be evaluated against {@link Column}s.
 *
 * These Columns must be partition columns, and will have ordinals matching the latest snapshot
 * schema.
 */
public class PartitionRow implements Row {

    private final StructType schema;
    private final Map<Integer, String> ordinalToValue;

    public PartitionRow(
            StructType schema,
            Map<String, Integer> partitionOrdinals,
            Map<String, String> partitionValuesMap) {
        this.ordinalToValue = new HashMap<>();
        for (Map.Entry<String, Integer> entry : partitionOrdinals.entrySet()) {
            final String partitionColumnName = entry.getKey();
            final int partitionColumnOrdinal = entry.getValue();
            final String partitionColumnValue = partitionValuesMap.get(partitionColumnName);
            ordinalToValue.put(partitionColumnOrdinal, partitionColumnValue);
        }
        this.schema = schema;
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return ordinalToValue.get(ordinal) == null;
    }

    @Override
    public boolean getBoolean(int ordinal) {
        return Boolean.parseBoolean(ordinalToValue.get(ordinal));
    }

    @Override
    public int getInt(int ordinal) {
        return Integer.parseInt(ordinalToValue.get(ordinal));
    }

    @Override
    public long getLong(int ordinal) {
        return Long.parseLong(ordinalToValue.get(ordinal));
    }

    @Override
    public String getString(int ordinal) {
        return ordinalToValue.get(ordinal);
    }

    @Override
    public Row getRecord(int ordinal) {
        throw new UnsupportedOperationException("Partition values can't be StructTypes");
    }

    @Override
    public <T> List<T> getList(int ordinal) {
        throw new UnsupportedOperationException("Partition values can't be Lists");
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal) {
        throw new UnsupportedOperationException("Partition values can't be Maps");
    }
}
