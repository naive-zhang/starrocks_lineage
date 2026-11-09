package com.starrocks.lineage.model;

/**
 * Raised when a name cannot be completed to its three-part form, e.g. a bare table name while the session
 * had no current database, or a name with more than three parts.
 */
public class UnresolvedNameException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public UnresolvedNameException(String message) {
        super(message);
    }
}
