package com.starrocks.sql.parser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StarRocksSqlParserTest {

    private final StarRocksSqlParser parser = new StarRocksSqlParser();

    static Stream<Arguments> statementsOfEveryInScopeType() {
        return Stream.of(
                Arguments.of("WITH c1 AS (SELECT a, b FROM db1.t1), c2 AS (SELECT a FROM c1 WHERE b > 1) "
                                + "SELECT a, SUM(a) OVER (PARTITION BY a ORDER BY a) FROM c2 "
                                + "UNION ALL SELECT x, y FROM t2 ORDER BY 1 LIMIT 10",
                        StarRocksParser.QueryStatementContext.class),
                Arguments.of("insert overwrite db.t2 (c1, c2) select a, b from db.t1 where a in (1, 2)",
                        StarRocksParser.InsertStatementContext.class),
                Arguments.of("INSERT INTO t2 SELECT t1.* FROM t1 JOIN t3 USING (id)",
                        StarRocksParser.InsertStatementContext.class),
                Arguments.of("CREATE TABLE IF NOT EXISTS db.ctas_t DISTRIBUTED BY HASH(a) BUCKETS 4 "
                                + "AS SELECT a, COUNT(*) AS cnt FROM db.t1 GROUP BY a HAVING COUNT(*) > 1",
                        StarRocksParser.CreateTableAsSelectStatementContext.class),
                Arguments.of("CREATE VIEW v1 (x, y) AS SELECT a, b FROM t1",
                        StarRocksParser.CreateViewStatementContext.class),
                Arguments.of("ALTER VIEW v1 AS SELECT a FROM t1",
                        StarRocksParser.AlterViewStatementContext.class),
                Arguments.of("CREATE MATERIALIZED VIEW mv1 DISTRIBUTED BY HASH(k) REFRESH ASYNC "
                                + "AS SELECT k, SUM(v) AS s FROM t1 GROUP BY k",
                        StarRocksParser.CreateMaterializedViewStatementContext.class),
                Arguments.of("UPDATE t1 SET b = t2.b + 1 FROM t2 WHERE t1.id = t2.id",
                        StarRocksParser.UpdateStatementContext.class),
                Arguments.of("DELETE FROM t1 USING t2 WHERE t1.id = t2.id",
                        StarRocksParser.DeleteStatementContext.class),
                Arguments.of("SUBMIT TASK task1 AS INSERT INTO t2 SELECT * FROM t1",
                        StarRocksParser.SubmitTaskStatementContext.class),
                Arguments.of("SELECT id, e1, e2 FROM t1, UNNEST(arr1, arr2) AS u(e1, e2)",
                        StarRocksParser.QueryStatementContext.class),
                Arguments.of("SELECT array_map((x, y) -> x + y, col1, col2) FROM t1",
                        StarRocksParser.QueryStatementContext.class),
                Arguments.of("SELECT c1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
                        StarRocksParser.QueryStatementContext.class),
                Arguments.of("SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c) AS rn FROM t1 "
                                + "QUALIFY rn = 1",
                        StarRocksParser.QueryStatementContext.class),
                Arguments.of("SELECT /*+ SET_VAR(query_timeout = 10) */ CASE WHEN a > 0 THEN b ELSE c END FROM "
                                + "hive_catalog.db.t1 t",
                        StarRocksParser.QueryStatementContext.class));
    }

    @ParameterizedTest
    @MethodSource("statementsOfEveryInScopeType")
    void officialGrammarParsesEveryInScopeStatement(String sql, Class<?> expectedContext) {
        StarRocksSqlParser.ParsedSql parsed = parser.parse(sql);
        assertEquals(1, parsed.getStatements().size());
        StarRocksParser.StatementContext statement = parsed.getStatements().get(0);
        assertInstanceOf(expectedContext, statement.getChild(0), () -> "statement was " + statement.getText());
        assertNotNull(parsed.getPredictionMode());
    }

    @Test
    void keywordsAreCaseInsensitiveButIdentifierTextIsPreserved() {
        StarRocksSqlParser.ParsedSql parsed = parser.parse("sElEcT MyCol FrOm MyDb.MyTable");
        StarRocksParser.QuerySpecificationContext spec = firstQuerySpecification(parsed);
        assertEquals("MyCol", spec.selectItem(0).getText());
        assertEquals(PredictionMode.SLL, parsed.getPredictionMode());
    }

    @Test
    void pipesAreLogicalOrByDefaultAndConcatUnderPipesAsConcat() {
        assertEquals(StarRocksParser.LOGICAL_OR, typeOfPipes(SqlModeHelper.MODE_DEFAULT));
        assertEquals(StarRocksParser.CONCAT,
                typeOfPipes(SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_PIPES_AS_CONCAT));
        // and the parser accepts both forms
        new StarRocksSqlParser(SqlModeHelper.MODE_PIPES_AS_CONCAT).parse("SELECT a || b FROM t");
        parser.parse("SELECT a || b FROM t");
    }

    private static int typeOfPipes(long sqlMode) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString("a || b")));
        lexer.setSqlMode(sqlMode);
        List<Token> tokens = new ArrayList<>();
        for (Token t = lexer.nextToken(); t.getType() != Token.EOF; t = lexer.nextToken()) {
            if (t.getChannel() == Token.DEFAULT_CHANNEL) {
                tokens.add(t);
            }
        }
        assertEquals(3, tokens.size());
        return tokens.get(1).getType();
    }

    @Test
    void multipleStatementsAndEmptyStatementsAreSplit() {
        StarRocksSqlParser.ParsedSql parsed = parser.parse("SELECT 1; ; SELECT a FROM t;");
        assertEquals(2, parsed.getStatements().size());
    }

    @Test
    void syntaxErrorIsReportedWithPosition() {
        SqlParsingException e = assertThrows(SqlParsingException.class,
                () -> parser.parse("SELECT a FROM\n t WHERE WHERE"));
        assertEquals(2, e.getLine());
        assertEquals("WHERE", e.getOffendingText());
        assertTrue(e.getMessage().contains("line 2:"));
    }

    @Test
    void lexicalErrorIsReportedWithoutFallbackConfusion() {
        SqlParsingException e = assertThrows(SqlParsingException.class, () -> parser.parse("SELECT 'abc FROM t"));
        assertEquals(1, e.getLine());
    }

    @Test
    void parserIsThreadSafeAcrossConcurrentCalls() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                final int n = i;
                futures.add(pool.submit(() -> parser.parse(
                        "SELECT c" + n + ", SUM(v) FROM db.t" + n + " WHERE k = " + n + " GROUP BY c" + n)
                        .getStatements().size()));
            }
            for (Future<Integer> f : futures) {
                assertEquals(1, f.get());
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void dfaGuardResetsTheSharedCacheAboveTheThreshold() {
        StarRocksSqlParser guarded = new StarRocksSqlParser(SqlModeHelper.MODE_DEFAULT, 1, 1);
        long clearsBefore = StarRocksSqlParser.dfaClearCount();
        StarRocksSqlParser.ParsedSql parsed = guarded.parse(
                "SELECT a, COUNT(*) FROM t1 JOIN t2 ON t1.id = t2.id WHERE b > 1 GROUP BY a ORDER BY a");
        assertEquals(1, parsed.getStatements().size());
        assertTrue(StarRocksSqlParser.dfaClearCount() > clearsBefore);
        assertEquals(0, StarRocksSqlParser.cachedDfaStates(), "caches are empty right after a reset");
        // the parser keeps working with a cold cache
        assertEquals(1, guarded.parse("SELECT 1").getStatements().size());
        assertTrue(StarRocksSqlParser.cachedDfaStates() >= 0);
        assertThrows(IllegalArgumentException.class, () -> new StarRocksSqlParser(0, 0, 1));
    }

    private static StarRocksParser.QuerySpecificationContext firstQuerySpecification(
            StarRocksSqlParser.ParsedSql parsed) {
        StarRocksParser.QueryStatementContext q = parsed.getStatements().get(0).queryStatement();
        StarRocksParser.QueryPrimaryDefaultContext primary =
                (StarRocksParser.QueryPrimaryDefaultContext) q.queryRelation().queryNoWith().queryPrimary();
        return primary.querySpecification();
    }
}
