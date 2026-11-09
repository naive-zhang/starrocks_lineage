package com.starrocks.sql.parser;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe facade around the generated {@link StarRocksParser} implementing the two-stage strategy:
 *
 * <ol>
 *   <li><b>Lexing</b> is completed eagerly ({@link CommonTokenStream#fill()}) so that a lexical error is reported
 *       exactly once and is never confused with an SLL prediction failure.</li>
 *   <li><b>SLL</b> prediction with {@link BailErrorStrategy}: fast path, succeeds for the overwhelming
 *       majority of statements.</li>
 *   <li>On {@link ParseCancellationException} the token stream is rewound ({@code seek(0)}), the parser is
 *       {@link StarRocksParser#reset() reset} and the statement is parsed again with full <b>LL</b> prediction and
 *       the {@link DefaultErrorStrategy}, whose errors are turned into {@link SqlParsingException} by
 *       {@link AuditSqlErrorListener}.</li>
 * </ol>
 * <p>Each call creates its own lexer/parser instances; only ANTLR's internally synchronised DFA cache is shared.</p>
 *
 * <p><b>DFA cache guard.</b> The generated recognizers keep their prediction DFA in a static array that only grows.
 * StarRocks' grammar has ~3000 decisions and an audit stream contains millions of distinct statements, so in a
 * long-running service the cache grows without bound (observed in StarRocks FE itself). Every
 * {@code dfaCheckInterval} parses the total number of cached DFA states is measured and, above
 * {@code maxDfaStates}, both the parser and lexer caches are reset. Resetting replaces the array slots with fresh
 * {@link DFA} objects; threads still holding the old objects finish safely, they merely lose the warm cache.</p>
 */
public final class StarRocksSqlParser {

    /** Default upper bound of cached DFA states (parser + lexer), roughly a few hundred MB of heap. */
    public static final long DEFAULT_MAX_DFA_STATES = 300_000L;
    /** Default number of parses between two DFA size checks. */
    public static final long DEFAULT_DFA_CHECK_INTERVAL = 1_024L;

    private static final AtomicLong PARSE_COUNTER = new AtomicLong();
    private static final AtomicLong DFA_CLEAR_COUNTER = new AtomicLong();

    private final long sqlMode;
    private final long maxDfaStates;
    private final long dfaCheckInterval;

    /**
     * @param sqlMode bit mask from {@link SqlModeHelper}; controls e.g. whether {@code ||} is CONCAT
     */
    public StarRocksSqlParser(long sqlMode) {
        this(sqlMode, DEFAULT_MAX_DFA_STATES, DEFAULT_DFA_CHECK_INTERVAL);
    }

    /**
     * @param sqlMode          bit mask from {@link SqlModeHelper}
     * @param maxDfaStates     cached DFA states above which the caches are reset, must be &gt; 0
     * @param dfaCheckInterval number of parses between two checks, must be &gt; 0
     */
    public StarRocksSqlParser(long sqlMode, long maxDfaStates, long dfaCheckInterval) {
        if (maxDfaStates <= 0 || dfaCheckInterval <= 0) {
            throw new IllegalArgumentException("maxDfaStates and dfaCheckInterval must be > 0");
        }
        this.sqlMode = sqlMode;
        this.maxDfaStates = maxDfaStates;
        this.dfaCheckInterval = dfaCheckInterval;
    }

    /** Parser using {@link SqlModeHelper#MODE_DEFAULT}. */
    public StarRocksSqlParser() {
        this(SqlModeHelper.MODE_DEFAULT);
    }

    public long getSqlMode() {
        return sqlMode;
    }

    /**
     * Parses the given SQL text (one or more statements separated by {@code ;}).
     *
     * @param sql sanitized SQL text
     * @return the parse tree and diagnostics
     * @throws SqlParsingException when the text is not valid StarRocks 3.1 SQL
     */
    public ParsedSql parse(String sql) {
        Objects.requireNonNull(sql, "sql");
        long start = System.nanoTime();

        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.setSqlMode(sqlMode);
        lexer.removeErrorListeners();
        lexer.addErrorListener(AuditSqlErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();

        StarRocksParser parser = new StarRocksParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(AuditSqlErrorListener.INSTANCE);

        StarRocksParser.SqlStatementsContext tree;
        PredictionMode used;
        try {
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            parser.setErrorHandler(new BailErrorStrategy());
            try {
                tree = parser.sqlStatements();
                used = PredictionMode.SLL;
            } catch (ParseCancellationException sllFailure) {
                tokens.seek(0);
                parser.reset();
                parser.setErrorHandler(new DefaultErrorStrategy());
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parser.sqlStatements();
                used = PredictionMode.LL;
            }
        } finally {
            if (PARSE_COUNTER.incrementAndGet() % dfaCheckInterval == 0) {
                guardDfaCache(parser, lexer);
            }
        }
        return new ParsedSql(sql, tree, tokens, used, System.nanoTime() - start);
    }

    private void guardDfaCache(StarRocksParser parser, StarRocksLexer lexer) {
        long states = countStates(parser.getInterpreter().decisionToDFA)
                + countStates(lexer.getInterpreter().decisionToDFA);
        if (states > maxDfaStates) {
            parser.getInterpreter().clearDFA();
            lexer.getInterpreter().clearDFA();
            DFA_CLEAR_COUNTER.incrementAndGet();
        }
    }

    private static long countStates(DFA[] decisionToDfa) {
        long total = 0;
        for (DFA dfa : decisionToDfa) {
            total += dfa.states.size();
        }
        return total;
    }

    /** @return number of DFA states currently cached by the shared parser and lexer DFA arrays (monitoring) */
    public static long cachedDfaStates() {
        StarRocksLexer lexer = new StarRocksLexer(CharStreams.fromString(""));
        StarRocksParser parser = new StarRocksParser(new CommonTokenStream(lexer));
        return countStates(parser.getInterpreter().decisionToDFA)
                + countStates(lexer.getInterpreter().decisionToDFA);
    }

    /** @return how many times the DFA caches were reset by the guard since class loading (monitoring) */
    public static long dfaClearCount() {
        return DFA_CLEAR_COUNTER.get();
    }

    /**
     * Result of a successful parse.
     */
    public static final class ParsedSql {
        private final String sql;
        private final StarRocksParser.SqlStatementsContext tree;
        private final CommonTokenStream tokens;
        private final PredictionMode predictionMode;
        private final long elapsedNanos;

        ParsedSql(String sql, StarRocksParser.SqlStatementsContext tree, CommonTokenStream tokens,
                  PredictionMode predictionMode, long elapsedNanos) {
            this.sql = sql;
            this.tree = tree;
            this.tokens = tokens;
            this.predictionMode = predictionMode;
            this.elapsedNanos = elapsedNanos;
        }

        public String getSql() {
            return sql;
        }

        public StarRocksParser.SqlStatementsContext getTree() {
            return tree;
        }

        /** @return the token stream (needed to recover original text including whitespace) */
        public CommonTokenStream getTokens() {
            return tokens;
        }

        /** @return {@link PredictionMode#SLL} when the fast path succeeded, {@link PredictionMode#LL} otherwise */
        public PredictionMode getPredictionMode() {
            return predictionMode;
        }

        public long getElapsedNanos() {
            return elapsedNanos;
        }

        /**
         * @return the non-empty statements in source order (empty {@code ;} statements are dropped)
         */
        public List<StarRocksParser.StatementContext> getStatements() {
            List<StarRocksParser.StatementContext> result = new ArrayList<>();
            for (StarRocksParser.SingleStatementContext single : tree.singleStatement()) {
                if (single.statement() != null) {
                    result.add(single.statement());
                }
            }
            return Collections.unmodifiableList(result);
        }
    }
}
