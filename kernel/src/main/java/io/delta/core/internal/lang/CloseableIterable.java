package io.delta.core.internal.lang;

import io.delta.core.utils.CloseableIterator;

public interface CloseableIterable<T> {
    CloseableIterator<T> iterator();
}
