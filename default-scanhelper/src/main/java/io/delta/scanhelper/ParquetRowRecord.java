package io.delta.scanhelper;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ParquetRowRecord implements RowRecord
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
    public StructType getSchema()
    {
        return null;
    }

    @Override
    public int getLength()
    {
        return 0;
    }

    @Override
    public boolean isNullAt(String fieldName)
    {
        return values[getIndex(fieldName)] == null;
    }

    @Override
    public int getInt(String fieldName)
    {
        return (Integer) values[getIndex(fieldName)];
    }

    @Override
    public long getLong(String fieldName)
    {
        return (Long) values[getIndex(fieldName)];
    }

    @Override
    public byte getByte(String fieldName)
    {
        return (Byte) values[getIndex(fieldName)];
    }

    @Override
    public short getShort(String fieldName)
    {
        return (Short) values[getIndex(fieldName)];
    }

    @Override
    public boolean getBoolean(String fieldName)
    {
        return (Boolean) values[getIndex(fieldName)];
    }

    @Override
    public float getFloat(String fieldName)
    {
        return (Float) values[getIndex(fieldName)];
    }

    @Override
    public double getDouble(String fieldName)
    {
        return (Double) values[getIndex(fieldName)];
    }

    @Override
    public String getString(String fieldName)
    {
        return (String) values[getIndex(fieldName)];
    }

    @Override
    public byte[] getBinary(String fieldName)
    {
        return (byte[]) values[getIndex(fieldName)];
    }

    @Override
    public BigDecimal getBigDecimal(String fieldName)
    {
        return (BigDecimal) values[getIndex(fieldName)];
    }

    @Override
    public Timestamp getTimestamp(String fieldName)
    {
        return (Timestamp) values[getIndex(fieldName)];
    }

    @Override
    public Date getDate(String fieldName)
    {
        return (Date) values[getIndex(fieldName)];
    }

    @Override
    public RowRecord getRecord(String fieldName)
    {
        return (RowRecord) values[getIndex(fieldName)];
    }

    @Override
    public <T> List<T> getList(String fieldName)
    {
        return (List<T>) values[getIndex(fieldName)];
    }

    @Override
    public <K, V> Map<K, V> getMap(String fieldName)
    {
        return (Map<K, V>) values[getIndex(fieldName)];
    }

    private int getIndex(String columnName)
    {
        return Optional.of(columnNameToIndexMap.get(columnName))
                .orElseThrow(() -> new IllegalArgumentException("Unknown column name: " + columnName));
    }

    private static Map<String, Integer> constructColumnIndexMap(StructType schema) {
        // TODO: explore Java's zipWithIndex if available
        Map<String, Integer> columnNameToIndexMap = new HashMap<>();
        int index = 0;
        for (StructField field : schema.getFields()) {
            columnNameToIndexMap.put(field.getName(), index);
            index++;
        }
        return columnNameToIndexMap;
    }
}
