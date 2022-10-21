package io.delta.standalone.exceptions;

import io.delta.standalone.internal.DeltaColumnMappingMode;

// TODO: correct exception type to extend. Delta core extends AnalysisException
public class ColumnMappingException extends RuntimeException {
    private final DeltaColumnMappingMode mode;

    public ColumnMappingException(String msg, DeltaColumnMappingMode mode) {
        super(msg);
        this.mode = mode;
    }
}
