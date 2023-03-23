package io.delta.core.internal.checkpoint;

import java.util.Optional;

import io.delta.core.internal.lang.Ordered;
import io.delta.core.internal.util.FileNames;

public class CheckpointInstance implements Ordered<CheckpointInstance> {
    public static final CheckpointInstance MAX_VALUE = new CheckpointInstance(-1);

    public final long version;
    public final Optional<Integer> numParts;

    public CheckpointInstance(String path) {
        this(FileNames.getFileVersion(path));
    }

    public CheckpointInstance(long version) {
        this(version, Optional.empty());
    }

    public CheckpointInstance(long version, Optional<Integer> numParts) {
        this.version = version;
        this.numParts = numParts;
    }

    boolean isNotLaterThan(CheckpointInstance other) {
        if (other == CheckpointInstance.MAX_VALUE) return true;
        return version <= other.version;
    }

    @Override
    public int compareTo(CheckpointInstance that) {
        if (version == that.version) {
            return numParts.orElse(1) - that.numParts.orElse(1);
        } else {
            // we need to guard against overflow. We just can't return (this - that).toInt
            return version - that.version < 0 ? -1 : 1;
        }
    }
}
