package io.delta.core.data;

import io.delta.core.data.Row;
import io.delta.core.types.StructField;
import io.delta.core.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ParquetRowRecord implements Row
{
    private final StructType schema;
    private final Object[] values;
    private final Map<String, Integer> columnNameToIndexMap;

    public ParquetRowRecord(StructType schema, Object[] values) {
        this.schema = requireNonNull(schema, "schema is null");
        this.values = requireNonNull(values, "values is null");
        this.columnNameToIndexMap = constructColumnIndexMap(schema);
        // TODO: do a validation to make sure the values match with the schema
    }

    @Override
    public boolean isNullAt(int ordinal)
    {
        return values[ordinal] == null;
    }

    @Override
    public boolean getBoolean(int ordinal)
    {
        return (boolean) values[ordinal];
    }

    @Override
    public int getInt(int ordinal)
    {
        return (int) values[ordinal];
    }

    @Override
    public long getLong(int ordinal)
    {
        return (long) values[ordinal];
    }

    @Override
    public String getString(int ordinal)
    {
        return (String) values[ordinal];
    }

    @Override
    public Row getRecord(int ordinal)
    {
        return (Row) values[ordinal];
    }

    @Override
    public <T> List<T> getList(int ordinal)
    {
        return (List<T>) values[ordinal];
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal)
    {
        return (Map<K, V>) values[ordinal];
    }

    private static Map<String, Integer> constructColumnIndexMap(StructType schema) {
        // TODO: explore Java's zipWithIndex if available
        Map<String, Integer> columnNameToIndexMap = new HashMap<>();
        int index = 0;
        for (StructField field : schema.fields()) {
            columnNameToIndexMap.put(field.getName(), index);
            index++;
        }
        return columnNameToIndexMap;
    }
}
