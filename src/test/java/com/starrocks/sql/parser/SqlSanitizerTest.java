package com.starrocks.sql.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlSanitizerTest {

    @Test
    void removesJdbcDriverHeaderComment() {
        SqlSanitizer.SanitizedSql s = SqlSanitizer.sanitize(
                "/* ApplicationName=DBeaver 23.1.0 - SQLEditor <Script-3.sql> */ SELECT a FROM t;");
        assertEquals("SELECT a FROM t", s.getSql());
        assertEquals("SELECT", s.getLeadingKeyword());
        assertEquals(1, s.getRemovedComments());
        assertFalse(s.isManagementStatement());
    }

    @Test
    void keepsOptimizerHintsAndStringContent() {
        String sql = "SELECT /*+ SET_VAR(query_timeout=1) */ '-- not a comment', \"/* nor this */\", `a#b` "
                + "FROM t -- trailing\nWHERE x = 'it''s' # mysql comment\nAND y = 'a\\'b'";
        SqlSanitizer.SanitizedSql s = SqlSanitizer.sanitize(sql);
        assertEquals("SELECT /*+ SET_VAR(query_timeout=1) */ '-- not a comment', \"/* nor this */\", `a#b` "
                + "FROM t \nWHERE x = 'it''s' \nAND y = 'a\\'b'", s.getSql());
        assertEquals(2, s.getRemovedComments());
        new StarRocksSqlParser().parse(s.getSql());
    }

    @Test
    void removedCommentNeverGluesTokens() {
        assertEquals("SELECT a FROM t", SqlSanitizer.sanitize("SELECT/*x*/a/*y*/FROM/*z*/t").getSql());
    }

    @Test
    void leadingParenthesesAndHintsAreSkippedForClassification() {
        assertEquals("SELECT", SqlSanitizer.sanitize("((SELECT 1)) UNION (SELECT 2)").getLeadingKeyword());
        assertEquals("WITH", SqlSanitizer.sanitize("-- c\n WITH x AS (SELECT 1) SELECT * FROM x")
                .getLeadingKeyword());
        assertEquals("INSERT", SqlSanitizer.sanitize("/*+ hint */ insert into t select 1").getLeadingKeyword());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SHOW TABLES", "show databases", "DESC t", "DESCRIBE t", "SET query_timeout = 10", "USE db1",
        "ADMIN SHOW FRONTEND CONFIG", "EXPLAIN SELECT 1", "KILL 123", "/* ApplicationName=x */ set names utf8",
        "explain costs insert into t select 1"
    })
    void managementStatementsAreDetected(String sql) {
        assertTrue(SqlSanitizer.sanitize(sql).isManagementStatement(), sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT 1", "WITH a AS (SELECT 1) SELECT * FROM a", "INSERT INTO t SELECT 1", "UPDATE t SET a = 1",
        "DELETE FROM t WHERE a = 1", "CREATE VIEW v AS SELECT 1", "SUBMIT TASK AS INSERT INTO t SELECT 1"
    })
    void dataStatementsAreNotManagement(String sql) {
        assertFalse(SqlSanitizer.sanitize(sql).isManagementStatement(), sql);
    }

    @Test
    void nullAndCommentOnlyStatementsBecomeEmpty() {
        assertTrue(SqlSanitizer.sanitize(null).isEmpty());
        assertTrue(SqlSanitizer.sanitize("  /* only */ -- comment\n ;; ").isEmpty());
        assertTrue(SqlSanitizer.sanitize("﻿").isEmpty());
    }

    @Test
    void unterminatedBlockCommentIsDroppedToEnd() {
        assertEquals("SELECT 1", SqlSanitizer.sanitize("SELECT 1 /* never closed").getSql());
    }
}
