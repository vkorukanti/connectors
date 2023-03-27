package io.delta.core.data;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.core.types.DataType;
import io.delta.core.types.LongType;
import io.delta.core.types.StructField;
import io.delta.core.types.StructType;

public class JsonRow implements Row {

    // TODO: we can do this cleaner / smarter / better

    private static Object parse(ObjectNode rootNode, String fieldName, DataType dataType) {
        if (dataType instanceof LongType) return rootNode.get(fieldName).longValue();

        throw new UnsupportedOperationException(
            String.format("Unsupported DataType %s", dataType.typeName())
        );
    }

    private final Map<Integer, Object> ordinalToValueMap;
    private final StructType readSchema;

    public JsonRow(ObjectNode rootNode, StructType readSchema) {
        this.readSchema = readSchema;
        this.ordinalToValueMap = new HashMap<>();

        for (int i = 0; i < readSchema.length(); i++) {
            final StructField field = readSchema.at(i);
            Object val = parse(rootNode, field.name, field.dataType);
            ordinalToValueMap.put(i, val);
        }
    }

    @Override
    public long getLong(int ordinal) {
        assert (readSchema.at(ordinal).dataType instanceof LongType);
        return (long) ordinalToValueMap.get(ordinal);
    }
}
