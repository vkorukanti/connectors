package io.delta.kernel.internal.replay;

import java.net.URI;
import java.util.HashMap;
import java.util.Optional;

import io.delta.kernel.fs.Path;
import io.delta.kernel.internal.actions.Action;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.lang.FilteredCloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.utils.CloseableIterator;

public class ReverseActionsToAddFilesIterator
    extends FilteredCloseableIterator<AddFile, Tuple2<Action, Boolean>> {

    private final Path dataPath;
    private final CloseableIterator<Tuple2<Action, Boolean>> reverseActionIter;
    private final HashMap<UniqueFileActionTuple, RemoveFile> tombstonesFromJson;
    private final HashMap<UniqueFileActionTuple, AddFile> addFilesFromJson;

    public ReverseActionsToAddFilesIterator(
            Path dataPath,
            CloseableIterator<Tuple2<Action, Boolean>> reverseActionIter) {
        super(reverseActionIter);
        this.dataPath = dataPath;
        this.reverseActionIter = reverseActionIter;
        this.tombstonesFromJson = new HashMap<>();
        this.addFilesFromJson = new HashMap<>();
    }

    @Override
    protected Optional<AddFile> accept(Tuple2<Action, Boolean> element) {
        final Action action = element._1;
        final boolean isFromCheckpoint = element._2;

        if (action instanceof AddFile) {
            final AddFile add = ((AddFile) action)
                    .copyWithDataChange(false)
                    .withAbsolutePath(dataPath);
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
            final RemoveFile remove = ((RemoveFile) action)
                    .copyWithDataChange(false)
                    .withAbsolutePath(dataPath);
            final UniqueFileActionTuple key =
                new UniqueFileActionTuple(remove.toURI(), remove.getDeletionVectorUniqueId());

            tombstonesFromJson.put(key, remove);
        }

        return Optional.empty();
    }

    private Optional<AddFile> findNextValid() {
        while (reverseActionIter.hasNext()) {
            final Tuple2<Action, Boolean> tuple = reverseActionIter.next();

        }

        return Optional.empty();
    }

    private static class UniqueFileActionTuple extends Tuple2<URI, Optional<String>> {
        public UniqueFileActionTuple(URI fileURI, Optional<String> deletionVectorURI) {
            super(fileURI, deletionVectorURI);
        }
    }
}
