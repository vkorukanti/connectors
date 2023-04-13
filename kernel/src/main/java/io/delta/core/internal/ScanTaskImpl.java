package io.delta.core.internal;

import io.delta.core.ScanTask;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.InputFile;
import io.delta.core.fs.Path;
import io.delta.core.helpers.ConnectorReadContext;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.types.ArrayType;
import io.delta.core.types.DataType;
import io.delta.core.types.MapType;
import io.delta.core.types.StructField;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the {@link ScanTask}. This basically makes use the the {@link TableHelper}
 * to read the required data. In addition to delegating the read call to {@link TableHelper},
 * it also takes care of converting the logical schema to physical schema and pass the parrition
 * values to {@link TableHelper#readParquetFile}
 */
public class ScanTaskImpl implements ScanTask {

    private final Path dataPath;
    private final AddFile addFile;
    private final TableHelper tableHelper;
    private final StructType snapshotSchema;
    private final StructType partitionSchema;
    private final Map<String, String> configuration;

    public ScanTaskImpl(
            Path dataPath,
            AddFile addFile,
            Map<String, String> configuration,
            StructType snapshotSchema,
            StructType partitionSchema,
            TableHelper tableHelper) {
        this.dataPath = dataPath;
        this.addFile = addFile;
        this.tableHelper = tableHelper;
        this.configuration = configuration;
        this.snapshotSchema = snapshotSchema;
        this.partitionSchema = partitionSchema;
    }

    @Override
    public CloseableIterator<ColumnarBatch> getData(
            ConnectorReadContext readContext,
            StructType readSchema) throws IOException
    {
        return tableHelper.readParquetFile(
                readContext,
                new InputFile(new Path(dataPath, addFile.toPath()).toString()),
                convertToPhysicalSchema(readSchema, snapshotSchema),
                addFile.getPartitionValues());
    }

    @Override
    public CloseableIterator<ColumnarBatch> getData(ConnectorReadContext readContext)
            throws IOException
    {
        return tableHelper.readParquetFile(
                readContext,
                new InputFile(new Path(dataPath, addFile.toPath()).toString()),
                convertToPhysicalSchema(snapshotSchema, snapshotSchema),
                addFile.getPartitionValues());
    }

    /** Visible for testing */
    AddFile getAddFile() {
        return this.addFile;
    }

    /**
     * Helper method that converts the logical schema (requested by the connector) to physical
     * schema of the data stored in data files.
     */
    private StructType convertToPhysicalSchema(StructType logicalSchema, StructType snapshotSchema) {
        String columnMappingMode = configuration.get("delta.columnMapping.mode");
        if ("name".equalsIgnoreCase(columnMappingMode)) {
            StructType newSchema = new StructType();
            for (StructField field : logicalSchema.fields()) {
                DataType oldType = field.getDataType();
                StructField fieldFromMetadata = snapshotSchema.get(field.getName());
                DataType newType;
                if (oldType instanceof StructType) {
                    newType = convertToPhysicalSchema(
                            (StructType) field.getDataType(),
                            (StructType) fieldFromMetadata.getDataType());
                } else if (oldType instanceof ArrayType) {
                    throw new UnsupportedOperationException("NYI");
                } else if (oldType instanceof MapType) {
                    throw new UnsupportedOperationException("NYI");
                } else {
                    newType = oldType;
                }
                String physicalName = fieldFromMetadata.getMetadata()
                        .get("delta.columnMapping.physicalName");
                // TODO: validate physicalName
                newSchema = newSchema.add(physicalName, newType);
            }
            return newSchema;
        }
        if ("id".equalsIgnoreCase(columnMappingMode)) {
            StructType newSchema = new StructType();
            for (StructField field : logicalSchema.fields()) {
                DataType oldType = field.getDataType();
                StructField fieldFromMetadata = snapshotSchema.get(field.getName());
                DataType newType;
                if (oldType instanceof StructType) {
                    newType = convertToPhysicalSchema(
                            (StructType) field.getDataType(),
                            (StructType) fieldFromMetadata.getDataType());
                } else if (oldType instanceof ArrayType) {
                    throw new UnsupportedOperationException("NYI");
                } else if (oldType instanceof MapType) {
                    throw new UnsupportedOperationException("NYI");
                } else {
                    newType = oldType;
                }
                Map<String, String> newMetadata = new HashMap<>(fieldFromMetadata.getMetadata());
                String physicalName = newMetadata.remove("delta.columnMapping.physicalName");
                String columnId = newMetadata.remove("delta.columnMapping.id");
                newMetadata.put("parquet.field.id", columnId);
                // TODO: validate physicalName and columnId
                newSchema = newSchema.add(physicalName, newType, newMetadata);
            }
            return newSchema;
        }

        return logicalSchema;
    }
}
