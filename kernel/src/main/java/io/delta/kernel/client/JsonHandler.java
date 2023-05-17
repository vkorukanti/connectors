package io.delta.kernel.client;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

/**
 * Provides JSON handling functionality to Delta Kernel. Delta Kernel can use this client to
 * parse JSON strings into {@link io.delta.kernel.data.Row}. Connectors can leverage
 * this interface to provide their best implementation of the JSON parsing capability to
 * Delta Kernel.
 */
public interface JsonHandler
{
    /**
     * Parse the given <i>json</i> string and return the requested fields in given <i>schema</i>
     * as a {@link Row}.
     *
     * @param json Valid JSON object in string format.
     * @param schema Schema of the data to return from the parse JSON. If any requested fields
     *               are missing in the JSON string, a <i>null</i> is returned for that particular
     *               field in returned {@link Row}. The type for each given field is expected to
     *               match the type in JSON.
     * @return
     */
    Row parseJson(String json, StructType schema);
}
