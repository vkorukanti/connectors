package io.delta.core.data;

import io.delta.core.types.StructField;
import io.delta.core.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ColumnarBatch} wrapper around list of {@link Row} objects.
 */
public class DefaultColumnarBatch implements ColumnarBatch
{
    private final StructType schema;
    private final List<Row> rows;
    private final Map<Integer, String> columnIndexToNameMap;

    public DefaultColumnarBatch(StructType schema, List<Row> rows) {
        this.schema = schema;
        this.rows = rows;
        this.columnIndexToNameMap = constructColumnIndexMap(schema);
        // TODO: do a validation to make sure the values match with the schema
    }

    @Override
    public int getSize()
    {
        return rows.size();
    }

    @Override
    public ColumnarBatch slice(int start, int end)
    {
        return null;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        String columnName = columnIndexToNameMap.get(ordinal);
        StructField field = schema.get(columnName);
        return new DefaultColumnVector(field.getDataType(), rows, ordinal);
    }

    private static Map<Integer, String> constructColumnIndexMap(StructType schema) {
        // TODO: explore Java's zipWithIndex if available
        Map<Integer, String> columnIndexToNameMap = new HashMap<>();
        int index = 0;
        for (StructField field : schema.fields()) {
            columnIndexToNameMap.put(index, field.getName());
            index++;
        }
        return columnIndexToNameMap;
    }
}
