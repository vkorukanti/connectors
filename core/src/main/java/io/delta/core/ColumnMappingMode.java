package io.delta.core;

public enum ColumnMappingMode {
    NONE("none"),
    ID_MAPPING("id"),
    NAME_MAPPING("name");

    private final String name;

    ColumnMappingMode(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
