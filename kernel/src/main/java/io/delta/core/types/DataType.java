package io.delta.core.types;

import java.util.Locale;

public abstract class DataType {

    public static DataType fromTypeName(String primitiveTypeName) {
        if (primitiveTypeName.equals(LongType.INSTANCE.typeName())) return LongType.INSTANCE;
        if (primitiveTypeName.equals(StringType.INSTANCE.typeName())) return StringType.INSTANCE;

        throw new IllegalArgumentException(String.format("Can't parse type %s", primitiveTypeName));
    }

    public String typeName() {
       String name = this.getClass().getSimpleName();
       if (name.endsWith("Type")) {
           name = name.substring(0, name.length() - 4);
       }
       return name.toLowerCase(Locale.ROOT);
    }
}

