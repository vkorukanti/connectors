package io.delta.core;

import io.delta.core.types.StructType;

/**
 * Represents the snapshot of a Delta table.
 */
public interface Snapshot {

    /**
     * @return version of this snapshot in the Delta table
     */
    long getVersion();

    /**
     * @return Schema of the Delta table at this snapshot.
     */
    StructType getSchema();

    /**
     * Create scan builder to allow construction of scans to read data from this snapshot.
     * @return an instance of {@link ScanBuilder}
     */
    ScanBuilder getScanBuilder();
}
