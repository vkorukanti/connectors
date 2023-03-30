package io.delta.core.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.core.types.*;

public class JsonRow implements Row {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Methods
    ////////////////////////////////////////////////////////////////////////////////

    private static Object decode(
            ObjectNode rootNode,
            String fieldName,
            DataType dataType,
            boolean isNullable) {
        if (rootNode.get(fieldName) == null) {
            if (isNullable) {
                return null;
            }

            throw new RuntimeException(
                String.format(
                    "Root node at key %s is null but field isn't nullable. Root node: %s",
                    fieldName,
                    rootNode
                )
            );
        }

        JsonNode jsonValue = rootNode.get(fieldName);

        if (dataType instanceof UnresolvedDataType) {
            if (jsonValue.isTextual()) {
                return jsonValue.textValue();
            } else if (jsonValue instanceof ObjectNode) {
                // TODO
            }
        }

        if (dataType instanceof BooleanType) {
            if (!jsonValue.isBoolean()) {
                throw new RuntimeException(
                    String.format("RootNode at %s isn't a boolean", fieldName)
                );
            }
            return jsonValue.booleanValue();
        }

        if (dataType instanceof IntegerType) {
            if (!jsonValue.isInt()) {
                throw new RuntimeException(
                    String.format("RootNode at %s isn't an int", fieldName)
                );
            }
            return jsonValue.intValue();
        }

        if (dataType instanceof LongType) {
            if (!jsonValue.isLong()) {
                throw new RuntimeException(
                    String.format("RootNode at %s isn't a long", fieldName)
                );
            }
            return jsonValue.longValue();
        }

        if (dataType instanceof StringType) {
            if (!jsonValue.isTextual()) {
                throw new RuntimeException(
                    String.format("RootNode at %s isn't a string", fieldName)
                );
            }
            return jsonValue.textValue();
        }

        if (dataType instanceof StructType) {
            if (!jsonValue.isObject()) {
                throw new RuntimeException(
                    String.format("RootNode at %s isn't an object", fieldName)
                );
            }
            return new JsonRow((ObjectNode) jsonValue, (StructType) dataType);
        }

        if (dataType instanceof ArrayType) {
            if (!jsonValue.isArray()) {
                throw new RuntimeException(
                    String.format("RootNode at %s isn't an array", fieldName)
                );
            }
            final ArrayType arrayType = ((ArrayType) dataType);
            final List<Object> output = new ArrayList<>();
            final ArrayNode array = (ArrayNode) jsonValue;

            for (Iterator<JsonNode> it = array.elements(); it.hasNext();) {
                JsonNode element = it.next();
                if (element.isObject()) {
                    output.add(
                        new JsonRow((ObjectNode) element, (StructType) arrayType.getElementType())
                    );
                }
            }
            return output;
        }

        if (dataType instanceof MapType) {
            // TODO:
            return null;
        }

        throw new UnsupportedOperationException(
            String.format("Unsupported DataType %s for RootNode %s", dataType.typeName(), jsonValue)
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private final ObjectNode rootNode;
    private final Object[] parsedValues;
    private final StructType readSchema;

    public JsonRow(ObjectNode rootNode, StructType readSchema) {
        this.rootNode = rootNode;
        this.readSchema = readSchema;
        this.parsedValues = new Object[readSchema.length()];

        for (int i = 0; i < readSchema.length(); i++) {
            final StructField field = readSchema.at(i);
            final Object parsedValue = decode(rootNode, field.name, field.dataType, field.nullable);
            parsedValues[i] = parsedValue;
        }
    }

    private void assertType(int ordinal, DataType expectedType) {
        final String actualTypeName = readSchema.at(ordinal).dataType.typeName();
        if (!actualTypeName.equals(expectedType.typeName()) &&
            !actualTypeName.equals(UnresolvedDataType.INSTANCE.typeName())) {
            throw new RuntimeException(
                String.format(
                    "Tried to read long at ordinal %s but actual data type is",
                    ordinal, readSchema.at(ordinal).dataType.typeName()
                )
            );
        }
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return parsedValues[ordinal] == null;
    }

    @Override
    public boolean getBoolean(int ordinal) {
        assertType(ordinal, BooleanType.INSTANCE);
        return (boolean) parsedValues[ordinal];
    }

    @Override
    public int getInt(int ordinal) {
        assertType(ordinal, IntegerType.INSTANCE);
        return (int) parsedValues[ordinal];
    }

    @Override
    public long getLong(int ordinal) {
        assertType(ordinal, LongType.INSTANCE);
        return (long) parsedValues[ordinal];
    }

    @Override
    public String getString(int ordinal) {
        assertType(ordinal, StringType.INSTANCE);
        return (String) parsedValues[ordinal];
    }

    @Override
    public Row getRecord(int ordinal) {
        assertType(ordinal, StructType.EMPTY_INSTANCE);
        return (JsonRow) parsedValues[ordinal];
    }

    @Override
    public <T> List<T> getList(int ordinal) {
        assertType(ordinal, ArrayType.EMPTY_INSTANCE);
        return (List<T>) parsedValues[ordinal];
    }

    @Override
    public String toString() {
        return "JsonRow{" +
            "rootNode=" + rootNode +
            ", parsedValues=" + parsedValues +
            ", readSchema=" + readSchema +
            '}';
    }
}
