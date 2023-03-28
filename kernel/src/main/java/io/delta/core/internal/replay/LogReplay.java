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
import io.delta.core.internal.lang.Tuple2;
import io.delta.core.utils.CloseableIterator;

public class LogReplay {

    private final TableHelper tableHelper;
    private final LogSegment logSegment;
    private final CloseableIterable<Action> reverseActionsIterable;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;

    public LogReplay(Path logPath, TableHelper tableHelper, LogSegment logSegment) {
        this.tableHelper = tableHelper;
        this.logSegment = logSegment;

        final Stream<FileStatus> allFiles = Stream.concat(logSegment.checkpoints.stream(), logSegment.deltas.stream());
        assertLogFilesBelongToTable(logPath, allFiles);

        this.reverseActionsIterable = new ReverseFilesToActionsIterable(tableHelper, allFiles);
        this.protocolAndMetadata = new Lazy<>(this::loadTableProtocolAndMetadata);
    }

    /////////////////
    // Public APIs //
    /////////////////

    public CloseableIterator<Action> getAddFiles() {

    }

    public Protocol getProtocol() {
        return protocolAndMetadata.get()._1;
    }

    public Metadata getMetadata() {
        return protocolAndMetadata.get()._2;
    }

    /////////////////////
    // Private Helpers //
    /////////////////////

    private Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata() {
        Protocol protocol = null;
        Metadata metadata = null;

        try (final CloseableIterator<Action> reverseIter = reverseActionsIterable.iterator()) {
            while (reverseIter.hasNext()) {
                final Action action = reverseIter.next();

                if (action instanceof Protocol && protocol == null) {
                    // We only need the latest protocol
                    protocol = (Protocol) action;

                    if (metadata != null) {
                        // Stop since we have found the latest Protocol and <etadata.
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
