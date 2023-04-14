package io.delta.core;

import io.delta.core.types.StructType;

public interface Snapshot {

    long getVersion();

    StructType getSchema();

    ScanBuilder getScanBuilder();

    ScanState getScanState();
}
