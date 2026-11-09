package com.starrocks.sql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

/**
 * Fail-fast error listener shared by the lexer and the parser.
 *
 * <p>ANTLR's default {@code ConsoleErrorListener} prints to stderr and lets the parser recover, which would
 * produce partial trees and therefore <em>wrong lineage</em>. Audit analysis must be all-or-nothing per
 * statement, so the first syntax error aborts the parse with a {@link SqlParsingException}.</p>
 *
 * <p>The listener is stateless and thread-safe; use {@link #INSTANCE}.</p>
 */
public final class AuditSqlErrorListener extends BaseErrorListener {

    public static final AuditSqlErrorListener INSTANCE = new AuditSqlErrorListener();

    private static final int MAX_OFFENDING_TEXT = 64;

    private AuditSqlErrorListener() {
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                            String msg, RecognitionException e) {
        String offending = null;
        if (offendingSymbol instanceof Token) {
            Token token = (Token) offendingSymbol;
            offending = token.getType() == Token.EOF ? "<EOF>" : token.getText();
            if (offending != null && offending.length() > MAX_OFFENDING_TEXT) {
                offending = offending.substring(0, MAX_OFFENDING_TEXT) + "...";
            }
        }
        String message = "Syntax error at line " + line + ":" + charPositionInLine
                + (offending == null ? "" : " near '" + offending + "'") + ": " + msg;
        throw new SqlParsingException(message, line, charPositionInLine, offending, e);
    }
}
