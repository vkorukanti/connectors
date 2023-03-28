package io.delta.core.internal.replay;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.delta.core.internal.actions.Action;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.internal.actions.RemoveFile;
import io.delta.core.internal.lang.Tuple2;
import io.delta.core.utils.CloseableIterator;

public class ReverseActionsToAddFilesIterator implements CloseableIterator<AddFile> {

    private final CloseableIterator<Action> reverseActionIter;

    // TODO: Minor performance optimization. check if the remove file is from a checkpoint. If it is
    // we can also exclude it, since there will be no AddFile for the same key in the checkpoint
    private HashMap<UniqueFileActionTuple, RemoveFile> tombstones;

    // TODO: Minor Performance optimization. check if the add file is from a checkpoint. If it's from a checkpoint, don't add it
    // to this map since
    // - if this is from a checkpoint, ths is the last file we will iterate over (why would we
    //   include the log file before a checkpoint?)
    // - if this is from a checkpoint, there won't be the same AddFile twice
    private HashMap<UniqueFileActionTuple, AddFile> alreadyReturnedFiles;

    private Optional<AddFile> nextValid;

    public ReverseActionsToAddFilesIterator(CloseableIterator<Action> reverseActionIter) {
        this.reverseActionIter = reverseActionIter;
        this.tombstones = new HashMap<>();
        this.alreadyReturnedFiles = new HashMap<>();
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
            final Action action = reverseActionIter.next();

            if (action instanceof AddFile) {
                final AddFile add = ((AddFile) action).copyWithDataChange(false);
                final UniqueFileActionTuple key =
                    new UniqueFileActionTuple(add.toURI(), add.getDeletionVectorUniqueId());
                final boolean alreadyDeleted = tombstones.containsKey(key);
                final boolean alreadyReturned = alreadyReturnedFiles.containsKey(key);

                if (!alreadyReturned) {
                    // TODO: checkpoint optimization

                    alreadyReturnedFiles.put(key, add);

                    if (!alreadyDeleted) {
                        return Optional.of(add);
                    }
                }
            } else if (action instanceof RemoveFile) {
                final RemoveFile remove = ((RemoveFile) action).copyWithDataChange(false);
                final UniqueFileActionTuple key =
                    new UniqueFileActionTuple(remove.toURI(), remove.getDeletionVectorUniqueId());

                // TODO: checkpoint optimization
                tombstones.put(key, remove);
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
