package io.delta.core.internal.util;

import java.util.function.Supplier;

public interface Logging {

    default void logInfo(String msg) {
        System.out.println(this.getClass() + " :: " + msg);
    }

    default void logInfo(Supplier<String> msg) {
        System.out.println(this.getClass() + " :: " + msg.get());
    }

    default void logDebug(String msg) {
        System.out.println(this.getClass() + " :: " + msg);
    }
    default void logDebug(Supplier<String> msg) {
        System.out.println(this.getClass() + " :: " + msg.get());
    }
}
