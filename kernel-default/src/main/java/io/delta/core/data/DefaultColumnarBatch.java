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
    private final Map<String, Integer> columnNameToIndexMap;

    public DefaultColumnarBatch(StructType schema, List<Row> rows) {
        this.schema = schema;
        this.rows = rows;
        this.columnNameToIndexMap = constructColumnIndexMap(schema);
        // TODO: do a validation to make sure the values match with the schema
    }

    @Override
    public ColumnVector getColumnVector(String columnName)
    {
        int columnOrdinal = columnNameToIndexMap.get(columnName);
        StructField field = schema.get(columnName);
        return new DefaultColumnVector(field.getDataType(), rows, columnOrdinal);
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
