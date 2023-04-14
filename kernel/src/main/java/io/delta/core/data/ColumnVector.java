package io.delta.core.data;

import io.delta.core.types.DataType;

public interface ColumnVector extends AutoCloseable {
    /**
     * Returns the data type of this column vector.
     */
    DataType getDataType();

    /**
     * Number of eleements in the vector
     */
    int getSize();

    /**
     * Cleans up memory for this column vector. The column vector is not usable after this.
     * <p>
     * This overwrites {@link AutoCloseable#close} to remove the
     * {@code throws} clause, as column vector is in-memory and we don't expect any exception to
     * happen during closing.
     */
    @Override
    void close();

    /**
     * Returns whether the value at {@code rowId} is NULL.
     */
    boolean isNullAt(int rowId);

    /**
     * Returns the boolean type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    boolean getBoolean(int rowId);
    /**
     * Returns the byte type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    byte getByte(int rowId);

    /**
     * Returns the short type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    short getShort(int rowId);

    /**
     * Returns the int type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    int getInt(int rowId);

    /**
     * Returns the long type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    long getLong(int rowId);

    /**
     * Returns the float type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    float getFloat(int rowId);

    /**
     * Returns the double type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    double getDouble(int rowId);

    /**
     * Returns the binary type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     */
    byte[] getBinary(int rowId);

    /**
     * Returns the string type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     */
    String getString(int rowId);

    /** TODO: Revisit the complex type access methods */
    Object getMap(int rowId);

    Object getStruct(int rowId);

    Object getArray(int rowId);
}
