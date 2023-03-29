package io.delta.core.types;

import io.delta.core.data.Row;

/**
 * e.g. IntegerType -> {"name":"as_int","type":"integer","nullable":true,"metadata":{}}
 * e.g. LongType -> {"name":"as_long","type":"long","nullable":true,"metadata":{}}
 * e.g. ArrayType(IntegerType) -> {"name":"as_array_int","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":true,"metadata":{}}
 * e.g. MapType(IntegerType) -> {"name":"a","type":{"type":"map","keyType":"integer","valueType":"integer","valueContainsNull":true},"nullable":true,"metadata":{}}
 */
public class UnresolvedDataType extends DataType {

    public static DataType fromRow(Row row) {
        try {
            final String typeName = row.getString(0);
            return DataType.fromTypeName(typeName);
        } catch (RuntimeException ex) {
            // TODO: parse ArrayType, etc.
            throw new RuntimeException("TODO");
        }
    }

}
