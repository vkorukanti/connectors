package io.delta.kernel.helpers;

/**
 * Opaque object allowing connectors to add custom file context which gets passed through Delta
 * core from the connector calls to Delta core API and to the connector implementation of the
 * {@link TableHelper}.
 */
public interface ScanFileContext
{
}
