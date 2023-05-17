package io.delta.kernel;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.internal.TableImpl;

public interface Table {

    /**
     * Instantiate table object for Delta Lake table at the given path.
     *
     * @param path location where the Delta table is present. Path needs to be fully qualified.
     * @param tableClient instance of {@link TableClient} to help the Delta core with planning and
     *               reading the table.
     * @return an instance of {@link Table} representing the Delta table at given path
     * @throws TableNotFoundException when there is no Delta table at the given path.
     */
    static Table forPath(String path, TableClient tableClient)
        throws TableNotFoundException
    {
        return TableImpl.forPath(path, tableClient);
    }

    /**
     * Get the snapshot of the table with given version.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param version table version number
     * @return an instance of {@link Snapshot} for given version
     * @throws TableVersionNotFoundException when the version is not valid
     */
    Snapshot getSnapshotAtVersion(TableClient tableClient, long version)
        throws TableVersionNotFoundException;

    /**
     * Get the latest snapshot of the table.
     * 
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return an instance of {@link Snapshot}
     */
    Snapshot getLatestSnapshot(TableClient tableClient);
}
