package io.delta.core.data;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.core.types.LongType;
import io.delta.core.types.StringType;
import io.delta.core.types.StructField;
import io.delta.core.types.StructType;

public class JsonRow implements Row {

    // TODO: we can do this cleaner / smarter / better
    private static Object parse(ObjectNode rootNode, StructField field) {
        if (rootNode.get(field.name) == null) {
            if (field.nullable) {
                return null;
            }

            throw new NullPointerException(
                String.format("Root node at key %s is null but field %s isn't nullable", rootNode, field)
            );
        }

        JsonNode jsonValue = rootNode.get(field.name);
        System.out.println("JsonRow > field " + field.name + " is NOT null " + jsonValue.toString());

        if (field.dataType instanceof LongType) {
            assert (jsonValue.isLong()) :
                String.format("RootNode at %s isn't a long", field.name);
            return jsonValue.longValue();
        }

        if (field.dataType instanceof StructType) {
            assert (jsonValue.isObject()) :
                String.format("RootNode at %s isn't a object", field.name);
            return (ObjectNode) jsonValue;
        }

        if (field.dataType instanceof StringType) {
            assert (jsonValue.isTextual()) :
                String.format("RootNode at %s isn't a string", field.name);
            return jsonValue.textValue();
        }

        throw new UnsupportedOperationException(
            String.format("Unsupported DataType %s", field.dataType.typeName())
        );
    }

    private final ObjectNode rootNode;
    private final Map<Integer, Object> ordinalToValueMap;
    private final StructType readSchema;

    public JsonRow(ObjectNode rootNode, StructType readSchema) {
        this.rootNode = rootNode;
        this.readSchema = readSchema;
        this.ordinalToValueMap = new HashMap<>();

        for (int i = 0; i < readSchema.length(); i++) {
            final StructField field = readSchema.at(i);
            Object val = parse(rootNode, field);
            ordinalToValueMap.put(i, val);
        }
    }

    @Override
    public long getLong(int ordinal) {
        assert (readSchema.at(ordinal).dataType instanceof LongType);
        return (long) ordinalToValueMap.get(ordinal);
    }

    @Override
    public String getString(int ordinal) {
        assert (readSchema.at(ordinal).dataType instanceof StringType);

        return (String) ordinalToValueMap.get(ordinal);
    }

    @Override
    public Row getRecord(int ordinal) {
        assert (readSchema.at(ordinal).dataType instanceof StructType);
        return new JsonRow(
            (ObjectNode) ordinalToValueMap.get(ordinal),
            (StructType) readSchema.at(ordinal).dataType
        );
    }

    @Override
    public String toString() {
        return "JsonRow{" +
            "rootNode=" + rootNode +
            ", ordinalToValueMap=" + ordinalToValueMap +
            ", readSchema=" + readSchema +
            '}';
    }
}
