package io.delta.core.helpers;

import io.delta.core.expressions.Expression;
import io.delta.core.expressions.Literal;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link ConnectorReadContext} for connectors using Hadoop based file reading.
 */
public class DefaultConnectorReadContext
    extends ConnectorReadContext
{
    // An example use case where the task read is passing a dynamic filter information.
    private final Expression dynamicFilter;

    public DefaultConnectorReadContext(Expression dynamicFilter)
    {
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
    }

    public Expression getDynamicFilter()
    {
        return dynamicFilter;
    }

    public static DefaultConnectorReadContext of()
    {
        return new DefaultConnectorReadContext(Literal.TRUE);
    }

    public static DefaultConnectorReadContext of(Expression dynamicFilter)
    {
        return new DefaultConnectorReadContext(dynamicFilter);
    }
}
