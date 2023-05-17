package io.delta.kernel.internal.lang;

import io.delta.kernel.utils.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class ListUtils {

    private ListUtils() { }

    public static <T> Tuple2<List<T>, List<T>> partition(List<T> list, Predicate<? super T> predicate) {
        final Map<Boolean, List<T>> partitionMap = list
            .stream()
            .collect(Collectors.partitioningBy(predicate));
        return new Tuple2<>(partitionMap.get(true), partitionMap.get(false));
    }
}
