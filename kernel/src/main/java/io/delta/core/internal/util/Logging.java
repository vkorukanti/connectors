package io.delta.core.internal.util;

import java.util.function.Supplier;

public interface Logging {
    default void logInfo(Supplier<String> msg) {
        System.out.println(msg.get());
    }
}
