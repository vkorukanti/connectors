package io.delta.kernel.internal.lang;

public interface Ordered<T> extends Comparable<T> {

    default boolean lessThan(T that) {
        return this.compareTo(that) < 0;
    }

    default boolean lessThanOrEqualTo(T that) {
        return this.compareTo(that) <= 0;
    }

    default boolean greaterThan(T that) {
        return this.compareTo(that) > 0;
    }

    default boolean greaterThanOrEqualTo(T that) {
        return this.compareTo(that) >= 0;
    }
}
