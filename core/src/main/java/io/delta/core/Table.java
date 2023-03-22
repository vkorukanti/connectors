package io.delta.core;

import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.TableImpl;

public interface Table {

    static Table forPath(String path, TableHelper helper) {
        return TableImpl.forPath(path, helper);
    }

    Snapshot getLatestSnapshot();
}
