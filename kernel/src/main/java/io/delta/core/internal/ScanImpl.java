package io.delta.core.internal;

import java.io.IOException;

import io.delta.core.Scan;
import io.delta.core.ScanTask;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.internal.replay.LogReplay;
import io.delta.core.utils.CloseableIterator;

public class ScanImpl implements Scan {

    private final LogReplay logReplay;

    public ScanImpl(LogReplay logReplay) {
        this.logReplay = logReplay;
    }

    @Override
    public CloseableIterator<ScanTask> getTasks() {
        return new CloseableIterator<ScanTask>() {
            final CloseableIterator<AddFile> addFileIter = logReplay.loadAddFiles();

            @Override
            public void close() throws IOException {

            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public ScanTask next() {
                return null;
            }
        }
    }
}
