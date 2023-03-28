package io.delta.core.internal;

import java.io.IOException;

import io.delta.core.Scan;
import io.delta.core.ScanTask;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.internal.replay.LogReplay;
import io.delta.core.internal.util.ScanTaskImpl;
import io.delta.core.utils.CloseableIterator;

public class ScanImpl implements Scan {

    private final LogReplay logReplay;

    public ScanImpl(LogReplay logReplay) {
        this.logReplay = logReplay;
    }

    @Override
    public CloseableIterator<ScanTask> getTasks() {
        return new CloseableIterator<ScanTask>() {
            final CloseableIterator<AddFile> addFileIter = logReplay.getAddFiles();

            @Override
            public void close() throws IOException {
                addFileIter.close();
            }

            @Override
            public boolean hasNext() {
                return addFileIter.hasNext();
            }

            @Override
            public ScanTask next() {
                return new ScanTaskImpl(addFileIter.next());
            }
        };
    }
}
