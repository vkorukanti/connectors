package io.delta.core.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.delta.core.data.Row;

public final class StructType extends DataType {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    public static StructType EMPTY_INSTANCE = new StructType();

    public static StructType fromRow(Row row) {
        final List<Row> fields = row.getList(0);
        return new StructType(
            fields
                .stream()
                .map(StructField::fromRow)
                .collect(Collectors.toList())
        );
    }

    public static StructType READ_SCHEMA = new StructType()
        .add("fields", new ArrayType(StructField.READ_SCHEMA, false /* contains null */ ));

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private final List<StructField> fields;

    public StructType() {
        this(new ArrayList<>());
    }

    public StructType(List<StructField> fields) {
        this.fields = fields;
    }

    public StructType add(StructField field) {
        final List<StructField> fieldsCopy = new ArrayList<>(fields);
        fieldsCopy.add(field);

        return new StructType(fieldsCopy);
    }

    public StructType add(String name, DataType dataType) {
        return add(new StructField(name, dataType, true /* nullable */));
    }

    public List<StructField> fields() {
        return Collections.unmodifiableList(fields);
    }

    public List<String> fieldNames() {
        return fields.stream().map(f -> f.name).collect(Collectors.toList());
    }

    public int length() {
        return fields.size();
    }

    public StructField at(int index) {
        return fields.get(index);
    }

    @Override
    public String toString() {
        return String.format(
            "%s(%s)",
            getClass().getSimpleName(),
            fields.stream().map(StructField::toString).collect(Collectors.joining(", "))
        );
    }

    public String treeString() {
        return "TODO";
    }
}
