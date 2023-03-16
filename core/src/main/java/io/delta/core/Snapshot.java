package io.delta.core;

import io.delta.core.types.StructType;

public interface Snapshot {

    StructType getSchema();

    ScanBuilder getScanBuilder();
}
