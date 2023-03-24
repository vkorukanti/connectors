package io.delta.core.data;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.core.types.DataType;
import io.delta.core.types.LongType;
import io.delta.core.types.StructType;

public class JsonRow implements Row {

    // TODO: we can do this cleaner / smarter / better

    private static Object parse(JsonNode node, DataType dataType) {
        if (dataType instanceof LongType) return node.longValue();

        throw new UnsupportedOperationException(
            String.format("Unsupported DataType %s", dataType.typeName())
        );
    }

    private final Map<Integer, Object> ordinalToValueMap;

    private final StructType readSchema;

    public JsonRow(JsonNode json, StructType readSchema) {
        this.readSchema = readSchema;

        this.ordinalToValueMap = new HashMap<>();
        for (int i = 0; i < readSchema.length(); i++) {
            ordinalToValueMap.put(i, parse(json.get(i), readSchema.at(i).dataType));
        }
    }

    @Override
    public long getLong(int ordinal) {
        assert (readSchema.at(ordinal).dataType instanceof LongType);
        return (long) ordinalToValueMap.get(ordinal);
    }
}
