package com.starrocks.lineage.scope;

/**
 * Semantic failure while resolving names. Carries a machine readable {@link ErrorCode} so the analyzer can decide
 * between failing the statement ({@code ANALYSIS_ERROR}) and degrading gracefully.
 */
public class SemanticAnalysisException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Error classification.
     */
    public enum ErrorCode {
        /** Column not found in any visible relation of any enclosing scope. */
        COLUMN_NOT_FOUND,
        /** Unqualified column present in several visible relations. */
        AMBIGUOUS_COLUMN,
        /** Unqualified column absent from known schemas while several relations have an unknown schema. */
        AMBIGUOUS_UNKNOWN_SCHEMA,
        /** Qualifier ({@code t.} in {@code t.c} / {@code t.*}) matches no visible relation. */
        RELATION_NOT_FOUND,
        /** Two relations of the same query block share an alias / name. */
        DUPLICATE_RELATION,
        /** Column alias list size differs from the relation arity. */
        COLUMN_ALIAS_MISMATCH,
        /** Set-operation branches have different column counts. */
        SET_OPERATION_ARITY_MISMATCH,
        /** {@code GROUP BY 3} / {@code ORDER BY 3} outside the select list. */
        ORDINAL_OUT_OF_RANGE,
        /** Two CTEs with the same name in one WITH clause. */
        DUPLICATE_CTE,
        /** Scope stack misuse (pop of a non-top frame, overflow, operation in wrong scope kind). */
        SCOPE_STATE
    }

    private final ErrorCode errorCode;

    public SemanticAnalysisException(ErrorCode errorCode, String message) {
        super(errorCode + ": " + message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
