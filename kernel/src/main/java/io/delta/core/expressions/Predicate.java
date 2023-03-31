package io.delta.core.expressions;

import io.delta.core.types.BooleanType;
import io.delta.core.types.DataType;

/**
 * An {@link Expression} that defines a relation on inputs. Evaluates to true, false, or null.
 */
public interface Predicate extends Expression {
    @Override
    default DataType dataType() {
        return BooleanType.INSTANCE;
    }
}
