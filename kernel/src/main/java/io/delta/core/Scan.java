package io.delta.core;

import io.delta.core.utils.CloseableIterator;

public interface Scan {

    CloseableIterator<ScanTask> getTasks();
}
