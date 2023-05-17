package io.delta.kernel.client;

public class DefaultTableClient
        implements TableClient
{

    @Override
    public ExpressionHandler getExpressionHandler()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public JsonHandler getJsonHandler()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public FileSystemClient getFileSystemClient()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ParquetHandler getParquetHandler()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Create an instance of {@link DefaultTableClient}.
     * @return
     */
    public static DefaultTableClient create() {
        return new DefaultTableClient();
    }

//    private final Configuration hadoopConf;
//    private final LogStore logStore;
//    private final ObjectMapper objectMapper;
//
//    private DefaultTableClient() {
//        this.hadoopConf = new Configuration();
//        this.logStore = new LocalLogStore(hadoopConf);
//        this.objectMapper = new ObjectMapper();
//    }
//
//    @Override
//    public CloseableIterator<FileStatus> listFiles(FileStatus file)
//            throws FileNotFoundException
//    {
//        return new CloseableIterator<FileStatus>() {
//            private final Iterator<org.apache.hadoop.fs.FileStatus> iter;
//
//            {
//                try {
//                    iter = logStore.listFrom(new Path(file.getPath().toString()), hadoopConf);
//                } catch (IOException ex) {
//                    throw new RuntimeException("Could not resolve the FileSystem", ex);
//                }
//            }
//
//            @Override
//            public boolean hasNext() {
//                return iter.hasNext();
//            }
//
//            @Override
//            public FileStatus next() {
//                final org.apache.hadoop.fs.FileStatus impl = iter.next();
//                return new FileStatus(impl.getPath().toString(), impl.getLen(), impl.getModificationTime());
//            }
//
//            @Override
//            public void close() throws IOException { }
//        };
//    }
//
//    @Override
//    public CloseableIterator<ColumnarBatch> readJsonFile(FileStatus inputFile, StructType readSchema) throws FileNotFoundException {
//        return new CloseableIterator<ColumnarBatch>() {
//            private final io.delta.storage.CloseableIterator<String> iter;
//            private ColumnarBatch nextBatch;
//
//            {
//                try {
//                    iter = logStore.read(new Path(inputFile.getPath().toString()), hadoopConf);
//                } catch (IOException ex) {
//                    if (ex instanceof FileNotFoundException) {
//                        throw (FileNotFoundException) ex;
//                    }
//
//                    throw new RuntimeException("Could not resolve the FileSystem", ex);
//                }
//            }
//
//            @Override
//            public void close() throws IOException {
//                iter.close();
//            }
//
//            @Override
//            public boolean hasNext() {
//                if (nextBatch == null) {
//                    List<Row> rows = new ArrayList<>();
//                    for (int i = 0; i < 1024 && iter.hasNext(); i++) {
//                        // TODO: decide on the batch size
//                        rows.add(parseJson(iter.next(), readSchema));
//                    }
//                    if (rows.isEmpty()) {
//                        return false;
//                    }
//                    nextBatch = new DefaultColumnarBatch(readSchema, rows);
//                }
//                return true;
//            }
//
//            @Override
//            public ColumnarBatch next() {
//                // TODO: assert
//                ColumnarBatch toReturn = nextBatch;
//                nextBatch = null;
//                return toReturn;
//            }
//        };
//    }
//
//    @Override
//    public CloseableIterator<ColumnarBatch> readParquetFile(
//            FileStatus file,
//            Optional<ScanFileContext> scanFileContext,
//            ColumnMappingMode columnMappingMode,
//            StructType readSchema,
//            Map<String, String> partitionValues) throws IOException
//    {˜
//        StructType dataColumnSchema = removePartitionColumns(readSchema, partitionValues.keySet());
//        // DefaultScanFileContext defaultScanTaskContext = (DefaultScanFileContext) scanFileContext;
//        ParquetBatchReader batchReader = new ParquetBatchReader(hadoopConf);
//        return batchReader.read(file.getPath().toString(), dataColumnSchema);
//        // TODO: wrap the regular columnar batch iterator in a partition column generator
//    }
//
//    @Override
//    public DataInputStream readFile(FileStatus file) throws IOException
//    {
//        throw new UnsupportedOperationException("Not yet implemented");
//    }
//
//    @Override
//    public ExpressionEvaluator getExpressionEvaluator(StructType schema, Expression expression)
//    {
//        return new DefaultExpressionEvaluator(schema, expression);
//    }
//
//    @Override
//    public Row parseJson(String json, StructType readSchema) {
//        try {
//            final JsonNode jsonNode = objectMapper.readTree(json);
//            return new JsonRow((ObjectNode) jsonNode, readSchema);
//        } catch (JsonProcessingException ex) {
//            throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
//        }
//    }
//
//    private static StructType removePartitionColumns(
//            StructType readSchema,
//            Set<String> partitionColumns) {
//        StructType dataColumnSchema = new StructType();
//
//        for (StructField field : readSchema.fields()) {
//            if (!partitionColumns.contains(field.getName())) {
//                dataColumnSchema = dataColumnSchema.add(field);
//            }
//        }
//        return dataColumnSchema;
//    }
}
