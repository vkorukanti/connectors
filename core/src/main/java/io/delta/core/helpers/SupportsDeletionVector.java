package io.delta.core.helpers;

import java.io.DataInputStream;

/** Should only be extended by ScanHelper. */
public interface SupportsDeletionVector {

    DataInputStream readDeletionVector(String path);
}
