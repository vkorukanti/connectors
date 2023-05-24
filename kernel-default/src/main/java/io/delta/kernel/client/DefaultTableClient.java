package io.delta.kernel.client;

import org.apache.hadoop.conf.Configuration;

public class DefaultTableClient
        implements TableClient
{
    private final Configuration hadoopConf;

    private DefaultTableClient(Configuration hadoopConf)
    {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public ExpressionHandler getExpressionHandler()
    {
        return new DefaultExpressionHandler();
    }

    @Override
    public JsonHandler getJsonHandler()
    {
        return new DefaultJsonHandler(hadoopConf);
    }

    @Override
    public FileSystemClient getFileSystemClient()
    {
        return new DefaultFileSystemClient(hadoopConf);
    }

    @Override
    public ParquetHandler getParquetHandler()
    {
        return new DefaultParquetHandler(hadoopConf);
    }

    /**
     * Create an instance of {@link DefaultTableClient}.
     * @param hadoopConf Hadoop configuration to use.
     * @return
     */
    public static DefaultTableClient create(Configuration hadoopConf) {
        return new DefaultTableClient(hadoopConf);
    }
}
