package io.delta.core.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class StructType extends DataType {

    public static StructType READ_SCHEMA = new StructType()
        .add("fields", new ArrayType(StructField.READ_SCHEMA, false /* contains null */ ));

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

    public String treeString() {
        return "TODO";
    }
}
