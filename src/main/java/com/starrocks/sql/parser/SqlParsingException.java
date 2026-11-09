package com.starrocks.sql.parser;

/**
 * Raised when a statement cannot be parsed by the official StarRocks grammar.
 * Carries the 1-based line and 0-based column of the offending token for diagnostics.
 */
public class SqlParsingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int line;
    private final int charPositionInLine;
    private final String offendingText;

    public SqlParsingException(String message, int line, int charPositionInLine, String offendingText,
                               Throwable cause) {
        super(message, cause);
        this.line = line;
        this.charPositionInLine = charPositionInLine;
        this.offendingText = offendingText;
    }

    public int getLine() {
        return line;
    }

    public int getCharPositionInLine() {
        return charPositionInLine;
    }

    public String getOffendingText() {
        return offendingText;
    }
}
