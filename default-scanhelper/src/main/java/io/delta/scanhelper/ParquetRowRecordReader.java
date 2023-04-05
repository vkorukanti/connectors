package io.delta.scanhelper;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ParquetRowRecordReader {

    private final Configuration configuration;

    public ParquetRowRecordReader(Configuration configuration)
    {
        this.configuration = requireNonNull(configuration, "configuration is null");
    }

    public CloseableIterator<RowRecord> read(String path, StructType schema) {
        ParquetRecordReader<RowRecord> reader =
                new ParquetRecordReader<>(
                        new RowRecordReadSupport(schema),
                        FilterCompat.NOOP);

        Path filePath = new Path(path);
        try {
            FileSystem fs = filePath.getFileSystem(configuration);
            FileStatus fileStatus = fs.getFileStatus(filePath);
            reader.initialize(
                    new FileSplit(filePath, 0, fileStatus.getLen(), new String[0]),
                    configuration,
                    Reporter.NULL
            );
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        return new CloseableIterator<RowRecord>() {
            @Override
            public void close()
                    throws IOException
            {
                reader.close();
            }

            @Override
            public boolean hasNext()
            {
                try {
                    return reader.nextKeyValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public RowRecord next()
            {
                try {
                    return reader.getCurrentValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static class RowRecordReadSupport extends ReadSupport<RowRecord> {
        private final StructType readSchema;

        public RowRecordReadSupport(StructType readSchema)
        {
            this.readSchema = requireNonNull(readSchema, "readSchema is not null");
        }

        @Override
        public ReadContext init(InitContext context)
        {
            return new ReadContext(Utils.pruneSchema(context.getFileSchema(), readSchema));
        }

        @Override
        public RecordMaterializer<RowRecord> prepareForRead(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema,
                ReadContext readContext)
        {
            return new RowRecordMaterializer(readSchema);
        }
    }

    public static class RowRecordMaterializer extends RecordMaterializer<RowRecord> {
        private final StructType readSchema;
        private final RowRecordGroupConverter rowRecordGroupConverter;

        public RowRecordMaterializer(StructType readSchema) {
            this.readSchema = requireNonNull(readSchema, "readSchema is not null");
            this.rowRecordGroupConverter = new RowRecordGroupConverter(null, 0, readSchema);
        }

        @Override
        public void skipCurrentRecord()
        {
            super.skipCurrentRecord();
        }

        @Override
        public RowRecord getCurrentRecord()
        {
            return new ParquetRowRecord(readSchema, rowRecordGroupConverter.getCurrentRecord());
        }

        @Override
        public GroupConverter getRootConverter()
        {
            return rowRecordGroupConverter;
        }
    }

    public static class RowRecordGroupConverter extends GroupConverter {
        private final StructType readSchema;
        private final Converter[] converters;
        private final RowRecordGroupConverter parent;
        private final int fieldIndex;

        private Object[] currentRecordValues;

        public RowRecordGroupConverter(
                RowRecordGroupConverter parent,
                int filedIndex,
                StructType readSchema)
        {
            this.parent = requireNonNull(parent, "parent is not null");
            this.fieldIndex = filedIndex;
            this.readSchema = requireNonNull(readSchema, "readSchema is not null");
            StructField[] fields = readSchema.getFields();
            this.converters = new Converter[fields.length];

            for (int i = 0; i < converters.length; i++) {
                final StructField field = fields[i];
                final DataType dataType = field.getDataType();
                if (dataType instanceof StructType) {
                    converters[i] = new RowRecordGroupConverter(this, i, (StructType) dataType);
                } else {
                    converters[i] = new RowRecordPrimitiveConverter(this, i, dataType);
                }
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            // TODO: error check
            return converters[fieldIndex];
        }

        @Override
        public void start()
        {
            this.currentRecordValues = new Object[converters.length];
        }

        @Override
        public void end()
        {
            for (Converter converter : converters) {
                if (!converter.isPrimitive()) {
                    converter.asGroupConverter().end();
                }
            }
            if (parent != null) {
                parent.set(fieldIndex, currentRecordValues);
            }
        }

        public void set(int fieldIndex, Object value)
        {
            // TODO: error check
            currentRecordValues[fieldIndex] = value;
        }

        public Object[] getCurrentRecord() {
            return currentRecordValues;
        }
    }

    public static class RowRecordPrimitiveConverter extends PrimitiveConverter
    {
        private final RowRecordGroupConverter parent;
        private final DataType dataType;
        private final int fieldIndex;

        public RowRecordPrimitiveConverter(
                RowRecordGroupConverter parent,
                int fieldIndex,
                DataType dataType)
        {
            this.parent = requireNonNull(parent, "parent is not null");
            this.fieldIndex = requireNonNull(fieldIndex, "fieldIndex is not null");
            this.dataType = requireNonNull(dataType, "dataType is not null");
        }

        @Override
        public void addBinary(Binary value)
        {
            parent.set(fieldIndex, value);
        }

        @Override
        public void addBoolean(boolean value)
        {
            parent.set(fieldIndex, value);
        }

        @Override
        public void addDouble(double value)
        {
            parent.set(fieldIndex, value);
        }

        @Override
        public void addFloat(float value)
        {
            parent.set(fieldIndex, value);
        }

        @Override
        public void addInt(int value)
        {
            parent.set(fieldIndex, value);
        }

        @Override
        public void addLong(long value)
        {
            parent.set(fieldIndex, value);
        }
    }
}
