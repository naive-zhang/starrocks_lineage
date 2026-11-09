package com.starrocks.sql.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.misc.Interval;

import java.util.Objects;

/**
 * Upper-cases the look-ahead seen by the lexer while preserving the original text.
 *
 * <p>Every keyword in the official {@code StarRocksLex.g4} is written in upper case ({@code SELECT: 'SELECT';})
 * and the grammar contains no {@code caseInsensitive} option, so StarRocks feeds the lexer through exactly this
 * kind of wrapper. {@link #getText(Interval)} delegates to the wrapped stream, therefore identifiers, string
 * literals and backquoted names keep their original spelling in {@code Token#getText()}.</p>
 */
public final class CaseInsensitiveStream implements CharStream {

    private final CharStream stream;

    public CaseInsensitiveStream(CharStream stream) {
        this.stream = Objects.requireNonNull(stream, "stream");
    }

    @Override
    public String getText(Interval interval) {
        return stream.getText(interval);
    }

    @Override
    public void consume() {
        stream.consume();
    }

    @Override
    public int LA(int i) {
        int result = stream.LA(i);
        switch (result) {
            case 0:
            case IntStream.EOF:
                return result;
            default:
                // Character.toUpperCase(int) operates on code points, so supplementary characters are safe.
                return Character.toUpperCase(result);
        }
    }

    @Override
    public int mark() {
        return stream.mark();
    }

    @Override
    public void release(int marker) {
        stream.release(marker);
    }

    @Override
    public int index() {
        return stream.index();
    }

    @Override
    public void seek(int index) {
        stream.seek(index);
    }

    @Override
    public int size() {
        return stream.size();
    }

    @Override
    public String getSourceName() {
        return stream.getSourceName();
    }
}
