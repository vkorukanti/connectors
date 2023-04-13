package io.delta.core.data;

import static java.util.Objects.requireNonNull;

/**
 * Class encapsulating the info of the file to read.
 */
public class InputFile
{
    private final String path;

    /**
     * Create a {@link InputFile} object.
     * @param path Fully qualified file path
     * // TODO: we could add more info such as file length, split info later on
     */
    public InputFile(String path)
    {
        this.path = requireNonNull(path, "path is null");
    }

    public String getPath()
    {
        return path;
    }
}
