package io.delta.kernel.internal.lang;

import io.delta.kernel.utils.CloseableIterator;

public interface CloseableIterable<T> {
    CloseableIterator<T> iterator();
}
