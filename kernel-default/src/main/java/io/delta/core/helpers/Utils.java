package io.delta.core.helpers;

import io.delta.core.types.StructType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class Utils
{
    private Utils() {
    }

    /**
     * Given the file schema in Parquet file and selected columns by Delta, return
     * a subschema of the file schema.
     * @param fileSchema
     * @param deltaType
     * @return
     */
    public static final MessageType pruneSchema(
            MessageType fileSchema, // parquet
            StructType deltaType) // delta-core
    {
        // TODO: Handle the case where the column is not in Parquet file
        return deltaType.fields().stream()
                .map(column -> {
                    Type type = findStructField(fileSchema, column.getName());
                    return new MessageType(column.getName(), type);
                })
                .reduce(MessageType::union)
                .get();
    }

    private static Type findStructField(MessageType fileSchema, String columnName)
    {
        // TODO: we need to provide a way to search by id.
        if (fileSchema.containsField(columnName)) {
            return fileSchema.getType(columnName);
        }
        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (org.apache.parquet.schema.Type type : fileSchema.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }

        return null;
    }
}
