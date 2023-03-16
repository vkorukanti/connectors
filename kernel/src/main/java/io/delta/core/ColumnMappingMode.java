package io.delta.core;

/**
 * Column mapping mode that indicates how the logical column names should
 * be mapped to physical columns in the data Parquet file.
 */
public enum ColumnMappingMode {
    /** Logical and physical column names are the same.*/
    NONE("none"),
    /** Columns in data Parquet files are looked up using the id of the logical column */
    ID_MAPPING("id"),
    /** Columns in data Parquet files are looked up using the physical column name */
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
