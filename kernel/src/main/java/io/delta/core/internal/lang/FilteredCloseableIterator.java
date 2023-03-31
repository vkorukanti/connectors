package io.delta.core.internal.lang;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.delta.core.utils.CloseableIterator;

public abstract class FilteredCloseableIterator<RETURN_TYPE, ITER_TYPE> implements CloseableIterator<RETURN_TYPE> {

    private final CloseableIterator<ITER_TYPE> iter;
    private Optional<RETURN_TYPE> nextValid;
    private boolean closed;

    public FilteredCloseableIterator(CloseableIterator<ITER_TYPE> iter) {
        this.iter = iter;
        this.nextValid = Optional.empty();
        this.closed = false;
    }

    protected abstract Optional<RETURN_TYPE> accept(ITER_TYPE element);

    @Override
    public final boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Can't call `hasNext` on a closed iterator.");
        }
        if (!nextValid.isPresent()) {
            nextValid = findNextValid();
        }
        return nextValid.isPresent();
    }

    @Override
    public final RETURN_TYPE next() {
        if (closed) {
            throw new IllegalStateException("Can't call `next` on a closed iterator.");
        }
        if (!hasNext()) throw new NoSuchElementException();

        // By the definition of hasNext, we know that nextValid is non-empty

        final RETURN_TYPE ret = nextValid.get();
        nextValid = Optional.empty();
        return ret;
    }

    @Override
    public final void close() throws IOException {
        iter.close();
        this.closed = true;
    }

    private Optional<RETURN_TYPE> findNextValid() {
        while (iter.hasNext()) {
            final Optional<RETURN_TYPE> acceptedElementOpt = accept(iter.next());
            if (acceptedElementOpt.isPresent()) return acceptedElementOpt;
        }

        return Optional.empty();
    }
}
