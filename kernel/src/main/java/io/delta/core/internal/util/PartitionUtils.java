package io.delta.core.internal.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.delta.core.expressions.Expression;
import io.delta.core.internal.lang.Tuple2;
import io.delta.core.types.StructType;

public class PartitionUtils {

    private PartitionUtils() { }

    public static Map<String, Integer> getPartitionOrdinals(
            StructType snapshotSchema,
            StructType partitionSchema) {
        final Map<String, Integer> output = new HashMap<>();
        partitionSchema
            .fieldNames()
            .forEach(fieldName -> output.put(fieldName, snapshotSchema.indexOf(fieldName)));

        return output;
    }

    /**
     * Partition the given condition into two optional conjunctive predicates M, D such that
     * condition = M AND D, where we define:
     * - M: conjunction of predicates that can be evaluated using metadata only.
     * - D: conjunction of other predicates.
     */
    public static Tuple2<Optional<Expression>, Optional<Expression>> splitMetadataAndDataPredicates(
            Expression condition,
            List<String> partitionColumns) {
        return new Tuple2<>(Optional.of(condition), Optional.empty());
    }

    private static List<Expression> splitConjunctivePredicates(Expression condition) {
        return null;
    }

//    private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
//        condition match {
//            case a: And => splitConjunctivePredicates(a.getLeft) ++ splitConjunctivePredicates(a.getRight)
//            case other => other :: Nil
//        }
//    }
}
