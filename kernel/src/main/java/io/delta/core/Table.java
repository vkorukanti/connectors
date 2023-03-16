package io.delta.core;

import io.delta.core.helpers.ScanEnvironment;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.TableImpl;

public interface Table {

    /**
     * Instantiate table object for Delta Lake table at the given path.
     *
     * @param path location where the Delta table is present. Path needs to be fully qualified.
     * @param helper instance of {@link TableHelper} to help the Delta core with planning and
     *               reading the table.
     * @param scanEnvironment Connector specific scan environment to use. This is an opaque
     *                        object to Delta Core and it gets passed down to the given connector's
     *                        {@link TableHelper} instance.
     * @return an instance of {@link Table} representing the Delta table at given path
     * @throws TableNotFoundException when there is no Delta table at the given path.
     */
    static Table forPath(String path, TableHelper helper, ScanEnvironment scanEnvironment)
        throws TableNotFoundException
    {
        return TableImpl.forPath(path, helper, scanEnvironment);
    }

    /**
     * Get the snapshot of the table with given version.
     *
     * @param version table version number
     * @return an instance of {@link Snapshot} for given version
     * @throws TableVersionNotFoundException when the version is not valid
     */
    Snapshot getSnapshotAtVersion(long version)
        throws TableVersionNotFoundException;

    /**
     * Get the latest snapshot of the table.
     * @return an instance of {@link Snapshot}
     */
    Snapshot getLatestSnapshot();
}
