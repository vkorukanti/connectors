package io.delta.core.expressions;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.delta.core.data.Row;

/**
 * An {@link Expression} with two inputs and one output. The output is by default evaluated to null
 * if either input is evaluated to null.
 */
public abstract class BinaryExpression implements Expression {
    protected final Expression left;
    protected final Expression right;

    protected BinaryExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    @Override
    public final Object eval(Row row) {
        Object leftResult = left.eval(row);
        if (null == leftResult) return null;

        Object rightResult = right.eval(row);
        if (null == rightResult) return null;

        return nullSafeEval(leftResult, rightResult);
    }

    protected abstract Object nullSafeEval(Object leftResult, Object rightResult);

    @Override
    public List<Expression> children() {
        return Arrays.asList(left, right);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BinaryExpression that = (BinaryExpression) o;
        return Objects.equals(left, that.left) &&
            Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }
}
