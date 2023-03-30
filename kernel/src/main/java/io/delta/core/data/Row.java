package io.delta.core.data;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface Row {

    boolean isNullAt(int ordinal);
//
    boolean getBoolean(int ordinal);
//
//    byte getByte(int ordinal);
//
//    short getShort(int ordinal);
//
    int getInt(int ordinal);

    long getLong(int ordinal);

//    float getFloat(int ordinal);
//
//    double getDouble(int ordinal);
//
//    BigDecimal getDecimal(int ordinal, int precision, int scale);
//
    String getString(int ordinal);
//
//    byte[] getBinary(int ordinal);
//
//    Timestamp getTimestamp(int ordinal);
//
//    Date getDate(int ordinal);
//
    Row getRecord(int ordinal);
//
    <T> List<T> getList(int ordinal);
//
    <K, V> Map<K, V> getMap(int ordinal);
}
