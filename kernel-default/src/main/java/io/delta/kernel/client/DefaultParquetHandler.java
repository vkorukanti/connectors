package io.delta.kernel.client;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.parquet.ParquetFooter;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class DefaultParquetHandler
    implements ParquetHandler
{
    private final Configuration hadoopConf;

    public DefaultParquetHandler(Configuration hadoopConf)
    {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public ParquetFooter readParquetFooter(String filePath)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public CloseableIterator<Tuple2<FileStatus, FileReadContext>> contextualizeFileReads(
            CloseableIterator<FileStatus> fileIter,
            Expression predicate)
    {
        return new CloseableIterator<Tuple2<FileStatus, FileReadContext>>() {
            @Override
            public void close()
                    throws IOException
            {
                fileIter.close();
            }

            @Override
            public boolean hasNext()
            {
                return fileIter.hasNext();
            }

            @Override
            public Tuple2<FileStatus, FileReadContext> next()
            {
                return new Tuple2<>(fileIter.next(), new DefaultFileReadContext());
            }
        };
    }

    @Override
    public CloseableIterator<ParquetDataReadResult> readParquetFiles(
            CloseableIterator<Tuple2<FileStatus, FileReadContext>> fileIter,
            StructType physicalSchema) throws IOException
    {
        return new CloseableIterator<ParquetDataReadResult>() {
            private FileStatus currentFile;
            private CloseableIterator<ColumnarBatch> currentFileReader;

            @Override
            public void close()
                    throws IOException
            {
                if (currentFileReader != null) {
                    currentFileReader.close();
                }

                fileIter.close();

                // TODO: implement safe close of multiple closeables.
            }

            @Override
            public boolean hasNext()
            {
                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                if (currentFileReader == null || !currentFileReader.hasNext()) {
                    if (fileIter.hasNext()) {
                        Tuple2<FileStatus, FileReadContext> nextFileWithContext = fileIter.next();
                        currentFile = nextFileWithContext._1;
                        ParquetBatchReader batchReader = new ParquetBatchReader(hadoopConf);
                        currentFileReader = batchReader.read(currentFile.getPath(), physicalSchema);
                    } else {
                        return false;
                    }
                }

                return currentFileReader.hasNext();
            }

            @Override
            public ParquetDataReadResult next()
            {
                final ColumnarBatch data = currentFileReader.next();
                final FileStatus file = currentFile;
                return new ParquetDataReadResult() {
                    @Override
                    public ColumnarBatch getData()
                    {
                        return data;
                    }

                    @Override
                    public FileStatus getFileStatus()
                    {
                        return file;
                    }
                };
            }
        };
    }
}
