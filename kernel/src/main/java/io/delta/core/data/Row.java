package io.delta.core.data;

import java.util.List;
import java.util.Map;

public interface Row {

    boolean isNullAt(int ordinal);

    boolean getBoolean(int ordinal);

    int getInt(int ordinal);

    long getLong(int ordinal);

    String getString(int ordinal);

    Row getRecord(int ordinal);

    <T> List<T> getList(int ordinal);

    <K, V> Map<K, V> getMap(int ordinal);

}
