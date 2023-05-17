package io.delta.kernel.internal.lang;

import java.util.Optional;
import java.util.function.Supplier;

public class Lazy<T> {
    private final Supplier<T> supplier;
    private Optional<T> instance = Optional.empty();

    public Lazy(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public T get() {
        if (!instance.isPresent()) {
            instance = Optional.of(supplier.get());
        }

        return instance.get();
    }
}
