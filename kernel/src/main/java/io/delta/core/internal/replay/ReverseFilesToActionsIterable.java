package io.delta.core.internal.replay;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.core.data.Row;
import io.delta.core.fs.FileStatus;
import io.delta.core.fs.Path;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.actions.Action;
import io.delta.core.internal.actions.SingleAction;
import io.delta.core.internal.lang.CloseableIterable;
import io.delta.core.utils.CloseableIterator;

public class ReverseFilesToActionsIterable implements CloseableIterable<Action> {

    private final TableHelper tableHelper;
    private final List<FileStatus> reverseSortedFiles;

    public ReverseFilesToActionsIterable(TableHelper tableHelper, Stream<FileStatus> filesUnsorted) {
        this.tableHelper = tableHelper;
        this.reverseSortedFiles = filesUnsorted
            .sorted(Comparator.comparing((FileStatus a) -> a.getPath().getName()).reversed())
            .collect(Collectors.toList());
    }


    @Override
    public CloseableIterator<Action> iterator() {
        return new CloseableIterator<Action>() {
            private final Iterator<FileStatus> filesIter = reverseSortedFiles.iterator();
            private Optional<CloseableIterator<Action>> actionsIter = Optional.empty();

            @Override
            public boolean hasNext() {
                tryEnsureNextActionsIterIsReady();

                // By definition of tryEnsureNextActionsIterIsReady, we know that if actionsIter
                // is non-empty then it has a next element

                return actionsIter.isPresent();
            }

            @Override
            public Action next() {
                if (!hasNext()) throw new NoSuchElementException();

                // By the definition of hasNext, we know that actionsIter is non-empty

                return actionsIter.get().next();
            }

            @Override
            public void close() throws IOException {
                if (actionsIter.isPresent()) {
                    actionsIter.get().close();
                    actionsIter = Optional.empty();
                }
            }

            /**
             * If the current `actionsIter` has no more elements, this function finds the next non-empty
             * file in `filesIter` and uses it to set `actionsIter`.
             */
            private void tryEnsureNextActionsIterIsReady() {
                if (actionsIter.isPresent()) {
                    // This iterator already has a next element, so we can exit early;
                    if (actionsIter.get().hasNext()) return;

                    // Clean up resources
                    try {
                        actionsIter.get().close();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }


                    // Set this to empty since we don't know if there's a next file yet
                    actionsIter = Optional.empty();
                }

                // Search for the next non-empty file and use that iter
                while (filesIter.hasNext()) {
                    actionsIter = Optional.of(getNextActionsIter());

                    if (actionsIter.get().hasNext()) return;

                    // It was an empty file.// Clean up resources
                    try {
                        actionsIter.get().close();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }

                    // Set this to empty since we don't know if there's a next file yet
                    actionsIter = Optional.empty();
                }

            }

            /**
             * Requires that `filesIter.hasNext` is true
             */
            private CloseableIterator<Action> getNextActionsIter() {
                final Path nextPath = filesIter.next().getPath();

                try {
                    if (nextPath.getName().endsWith(".json")) {
                        System.out.println("Scott > ReverseFilesToActionsIterable > reading " + nextPath.getName());
                        return new RowToActionIterator(
                            tableHelper.readJsonFile(nextPath.toString(), SingleAction.READ_SCHEMA)
                        );
                    } else if (nextPath.getName().endsWith(".parquet")) {
                        System.out.println("Scott > ReverseFilesToActionsIterable > reading " + nextPath.getName());
                        return new RowToActionIterator(
                            tableHelper.readParquetFile(nextPath.toString(), SingleAction.READ_SCHEMA)
                        );
                    } else {
                        throw new IllegalStateException(
                            String.format("Unexpected log file path: %s", nextPath)
                        );
                    }
                } catch (FileNotFoundException ex) {
                    throw new RuntimeException(ex);
                }
            }
        };
    }

    private static class RowToActionIterator implements CloseableIterator<Action> {
        private final CloseableIterator<Row> impl;

        /** Requires that Row represents a SingleAction. */
        public RowToActionIterator(CloseableIterator<Row> impl) {
            this.impl = impl;
        }

        @Override
        public boolean hasNext() {
            return impl.hasNext();
        }

        @Override
        public Action next() {
            return SingleAction.fromRow(impl.next()).unwrap();
        }

        @Override
        public void close() throws IOException {
            impl.close();
        }
    }
}
