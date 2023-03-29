package io.delta.core.internal.replay;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.delta.core.internal.actions.Action;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.internal.actions.RemoveFile;
import io.delta.core.internal.lang.Tuple2;
import io.delta.core.utils.CloseableIterator;

public class ReverseActionsToAddFilesIterator implements CloseableIterator<AddFile> {

    private final CloseableIterator<Tuple2<Action, Boolean>> reverseActionIter;

    private final HashMap<UniqueFileActionTuple, RemoveFile> tombstonesFromJson;

    private final HashMap<UniqueFileActionTuple, AddFile> addFilesFromJson;

    private Optional<AddFile> nextValid;

    public ReverseActionsToAddFilesIterator(CloseableIterator<Tuple2<Action, Boolean>> reverseActionIter) {
        this.reverseActionIter = reverseActionIter;
        this.tombstonesFromJson = new HashMap<>();
        this.addFilesFromJson = new HashMap<>();
        this.nextValid = Optional.empty();
    }

    @Override
    public boolean hasNext() {
        if (!nextValid.isPresent()) {
            nextValid = findNextValid();
        }

        return nextValid.isPresent();
    }

    @Override
    public AddFile next() {
        if (!hasNext()) throw new NoSuchElementException();

        // By the definition of hasNext, we know that actionsIter is non-empty

        final AddFile ret = nextValid.get();
        nextValid = Optional.empty();
        return ret;
    }

    @Override
    public void close() throws IOException {
        reverseActionIter.close();
    }

    private Optional<AddFile> findNextValid() {
        while (reverseActionIter.hasNext()) {
            final Tuple2<Action, Boolean> tuple = reverseActionIter.next();
            final Action action = tuple._1;
            final boolean isFromCheckpoint = tuple._2;

            if (action instanceof AddFile) {
                final AddFile add = ((AddFile) action).copyWithDataChange(false);
                final UniqueFileActionTuple key =
                    new UniqueFileActionTuple(add.toURI(), add.getDeletionVectorUniqueId());
                final boolean alreadyDeleted = tombstonesFromJson.containsKey(key);
                final boolean alreadyReturned = addFilesFromJson.containsKey(key);

                if (!alreadyReturned) {
                    // Note: No AddFile will appear twice in a checkpoint, so we only need
                    //       non-checkpoint AddFiles in the set
                    if (!isFromCheckpoint) {
                        addFilesFromJson.put(key, add);
                    }

                    if (!alreadyDeleted) {
                        return Optional.of(add);
                    }
                }
            } else if (action instanceof RemoveFile && !isFromCheckpoint) {
                // Note: There's no reason to put a RemoveFile from a checkpoint into tombstones map
                //       since, when we generate a checkpoint, any corresponding AddFile would have
                //       been excluded
                final RemoveFile remove = ((RemoveFile) action).copyWithDataChange(false);
                final UniqueFileActionTuple key =
                    new UniqueFileActionTuple(remove.toURI(), remove.getDeletionVectorUniqueId());

                tombstonesFromJson.put(key, remove);
            }
        }

        return Optional.empty();
    }

    private static class UniqueFileActionTuple extends Tuple2<URI, Optional<String>> {
        public UniqueFileActionTuple(URI fileURI, Optional<String> deletionVectorURI) {
            super(fileURI, deletionVectorURI);
        }
    }
}
