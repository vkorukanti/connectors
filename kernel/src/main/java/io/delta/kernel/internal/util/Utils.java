package io.delta.kernel.internal.util;

import io.delta.kernel.utils.CloseableIterator;

import java.io.IOException;

public class Utils
{
    public static <T> CloseableIterator<T> singletonCloseableIterator(T elem)  {
        return new CloseableIterator<T>() {
            private boolean accessed;
            @Override
            public void close()
                    throws IOException
            {
                // nothing to close
            }

            @Override
            public boolean hasNext()
            {
                return !accessed
            }

            @Override
            public T next()
            {
                accessed = true;
                return elem;
            }
        };
    }
}
