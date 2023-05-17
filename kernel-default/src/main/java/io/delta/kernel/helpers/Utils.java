package io.delta.kernel.helpers;

import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
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
                    Type type = findStructField(fileSchema, column);
                    if (type == null) {
                        return null;
                    }
                    return new MessageType(column.getName(), type);
                })
                .filter(type -> type != null)
                .reduce(MessageType::union)
                .get();
    }

    private static Type findStructField(MessageType fileSchema, StructField column)
    {
        // TODO: we need to provide a way to search by id.
        final String columnName = column.getName();
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

        // Create a type and return.
        return parquetTypeFromDeltaType(column.getDataType());
    }

    private static Type parquetTypeFromDeltaType(DataType deltaType) {
//        if (deltaType.typeName().equalsIgnoreCase(IntegerType.INSTANCE.typeName())) {
//            return PrimitiveType
//        }
        return null;
    }
}
