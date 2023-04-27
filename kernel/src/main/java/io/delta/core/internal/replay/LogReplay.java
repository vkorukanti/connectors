package io.delta.core.internal.replay;

import java.io.IOException;
import java.util.stream.Stream;

import io.delta.core.fs.FileStatus;
import io.delta.core.fs.Path;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.actions.*;
import io.delta.core.internal.lang.CloseableIterable;
import io.delta.core.internal.lang.Lazy;
import io.delta.core.internal.snapshot.LogSegment;
import io.delta.core.utils.Tuple2;
import io.delta.core.utils.CloseableIterator;

public class LogReplay {

    private final LogSegment logSegment;
    private final CloseableIterable<Tuple2<Action, Boolean>> reverseActionsIterable;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;

    public LogReplay(Path logPath, TableHelper tableHelper, LogSegment logSegment) {
        this.logSegment = logSegment;

        final Stream<FileStatus> allFiles = Stream.concat(logSegment.checkpoints.stream(), logSegment.deltas.stream());
        assertLogFilesBelongToTable(logPath, allFiles);

        this.reverseActionsIterable = new ReverseFilesToActionsIterable(tableHelper, allFiles);
        this.protocolAndMetadata = new Lazy<>(this::loadTableProtocolAndMetadata);
    }

    /////////////////
    // Public APIs //
    /////////////////

    public Lazy<Tuple2<Protocol, Metadata>> lazyLoadProtocolAndMetadata() {
        return this.protocolAndMetadata;
    }

    public CloseableIterator<AddFile> getAddFiles() {
        final CloseableIterator<Tuple2<Action, Boolean>> reverseActionsIter = reverseActionsIterable.iterator();
        return new ReverseActionsToAddFilesIterator(reverseActionsIter);
    }

    /////////////////////
    // Private Helpers //
    /////////////////////

    private Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata() {
        Protocol protocol = null;
        Metadata metadata = null;

        try (final CloseableIterator<Tuple2<Action, Boolean>> reverseIter = reverseActionsIterable.iterator()) {
            while (reverseIter.hasNext()) {
                final Action action = reverseIter.next()._1;

                if (action instanceof Protocol && protocol == null) {
                    // We only need the latest protocol
                    protocol = (Protocol) action;

                    if (metadata != null) {
                        // Stop since we have found the latest Protocol and Metadata.
                        return new Tuple2<>(protocol, metadata);
                    }
                } else if (action instanceof Metadata && metadata == null) {
                    // We only need the latest Metadata
                    metadata = (Metadata) action;

                    if (protocol != null) {
                        // Stop since we have found the latest Protocol and Metadata.
                        return new Tuple2<>(protocol, metadata);
                    }
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Could not close iterator", ex);
        }

        if (protocol == null) {
            throw new IllegalStateException(
                String.format("No protocol found at version %s", logSegment.version)
            );
        }

        throw new IllegalStateException(
            String.format("No metadata found at version %s", logSegment.version)
        );
    }

    /**
     * Verifies that a set of delta or checkpoint files to be read actually belongs to this table.
     */
    private void assertLogFilesBelongToTable(Path logPath, Stream<FileStatus> allFiles) {
        // TODO:
    }
}
