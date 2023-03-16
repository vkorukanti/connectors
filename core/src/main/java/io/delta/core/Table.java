package io.delta.core;

import io.delta.core.helpers.TableHelper;

public interface Table {

    static Table forPath(String path, TableHelper helper) {
        return null;
    }

    Snapshot getLatestSnapshot();
}
