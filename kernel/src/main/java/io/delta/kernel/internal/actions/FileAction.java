package io.delta.kernel.internal.actions;

import java.net.URI;
import java.net.URISyntaxException;

import io.delta.kernel.fs.Path;
import io.delta.kernel.internal.lang.Lazy;

public abstract class FileAction implements Action {
    protected final String path;
    protected final boolean dataChange;
    private final Lazy<URI> pathAsUri;
    private final Lazy<Path> pathAsPath;

    public FileAction(String path, boolean dataChange) {
        this.path = path;
        this.dataChange = dataChange;

        this.pathAsUri = new Lazy<>(() -> {
            try {
                return new URI(path);
            } catch (URISyntaxException ex) {
                throw new RuntimeException(ex);
            }
        });

        this.pathAsPath = new Lazy<>(() -> new Path(path));
    }

    public String getPath() {
        return path;
    }

    public boolean isDataChange() {
        return dataChange;
    }

    public URI toURI() {
        return pathAsUri.get();
    }

    public Path toPath() {
        return pathAsPath.get();
    }

    public abstract FileAction copyWithDataChange(boolean dataChange);
}
