package io.delta.core.internal.data;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.internal.actions.Metadata;
import io.delta.core.internal.actions.Protocol;
import io.delta.core.types.ArrayType;
import io.delta.core.types.IntegerType;
import io.delta.core.types.MapType;
import io.delta.core.types.StringType;
import io.delta.core.types.StructType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Expose the {@link io.delta.core.internal.actions.Metadata}  as a {@link ColumnarBatch}.
 */
public class MetadataProtocolColumnarBatch
        extends PojoColumnarBatch
{
    private static final Map<Integer, Function<MetadataAndProtocol, Object>> ordinalToAccessor = new HashMap<>();
    private static final Map<Integer, String> ordinalToColName = new HashMap<>();
    private static final StructType schema = new StructType()
            .add("configuration", new MapType(StringType.INSTANCE, StringType.INSTANCE,false))
            .add("schemaString", StringType.INSTANCE)
            .add("partitionColumns", new ArrayType(StringType.INSTANCE, false))
            .add("minReaderVersion", IntegerType.INSTANCE)
            .add("minWriterVersion", IntegerType.INSTANCE);

    static {
        ordinalToAccessor.put(0, (a) -> a.getConfiguration());
        ordinalToAccessor.put(1, (a) -> a.getSchemaString());
        ordinalToAccessor.put(2, (a) -> a.getPartitionColumns());
        ordinalToAccessor.put(3, (a) -> a.getMinReaderVersion());
        ordinalToAccessor.put(4, (a) -> a.getMinWriterVersion());

        ordinalToColName.put(0, "configuration");
        ordinalToColName.put(1, "schemaString");
        ordinalToColName.put(2, "partitionColumns");
        ordinalToColName.put(3, "minReaderVersion");
        ordinalToColName.put(4, "minWriterVersion");
    }

    public MetadataProtocolColumnarBatch(Metadata metadata, Protocol protocol)
    {
        super(Collections.singletonList(new MetadataAndProtocol(metadata, protocol)),
                schema,
                ordinalToAccessor,
                ordinalToColName);
    }

    public static final class MetadataAndProtocol {
        private final Map<String, String> configuration;
        private final String schemaString;
        private final List<String> partitionColumns;
        private final int minReaderVersion;
        private final int minWriterVersion;

        public MetadataAndProtocol(Metadata metadata, Protocol protocol)
        {
            this.configuration = metadata.getConfiguration();
            this.schemaString = metadata.getSchemaString();
            this.partitionColumns = metadata.getPartitionColumns();
            this.minReaderVersion = protocol.getMinReaderVersion();
            this.minWriterVersion = protocol.getMinWriterVersion();
        }

        public Map<String, String> getConfiguration()
        {
            return configuration;
        }

        public String getSchemaString()
        {
            return schemaString;
        }

        public List<String> getPartitionColumns()
        {
            return partitionColumns;
        }

        public int getMinReaderVersion()
        {
            return minReaderVersion;
        }

        public int getMinWriterVersion()
        {
            return minWriterVersion;
        }
    }
}
