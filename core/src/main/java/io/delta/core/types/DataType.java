package io.delta.core.types;

import java.util.Locale;

public abstract class DataType {
    public String typeName() {
       String name = this.getClass().getSimpleName();
       if (name.endsWith("Type")) {
           name = name.substring(0, name.length() - 4);
       }
       return name.toLowerCase(Locale.ROOT);
    }
}

