package com.starrocks.sql.parser;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the "zero modification" rule: the grammar files must be byte-identical to the files of the official
 * StarRocks repository, branch-3.1, path {@code fe/fe-core/src/main/java/com/starrocks/sql/parser/}.
 * If upstream is re-synced, update the hashes together with {@code docs/stage1-architecture.md}.
 */
class OfficialGrammarIntegrityTest {

    private static final Path GRAMMAR_DIR = Paths.get("src", "main", "antlr4", "com", "starrocks", "sql", "parser");

    static final String STARROCKS_G4_SHA256 = "cc04fb2c3924d4748a712c60c44737ef53e47025db40aa15b9fea85b0e5902e3";
    static final String STARROCKS_LEX_G4_SHA256 = "207d0c01926b67e9e2ce56ec0cc928d84f920abe384d6d9ed88549a4f0d4d9b0";

    @Test
    void parserGrammarIsByteIdenticalToUpstream() throws Exception {
        assertEquals(STARROCKS_G4_SHA256, sha256(GRAMMAR_DIR.resolve("StarRocks.g4")));
    }

    @Test
    void lexerGrammarIsByteIdenticalToUpstream() throws Exception {
        assertEquals(STARROCKS_LEX_G4_SHA256, sha256(GRAMMAR_DIR.resolve("StarRocksLex.g4")));
    }

    @Test
    void lexerStillReferencesTheQeShim() throws Exception {
        String lex = new String(Files.readAllBytes(GRAMMAR_DIR.resolve("StarRocksLex.g4")), "UTF-8");
        assertTrue(lex.contains("com.starrocks.qe.SqlModeHelper.MODE_DEFAULT"));
        assertTrue(lex.contains("com.starrocks.qe.SqlModeHelper.MODE_PIPES_AS_CONCAT"));
        assertEquals(com.starrocks.sql.parser.SqlModeHelper.MODE_PIPES_AS_CONCAT,
                com.starrocks.qe.SqlModeHelper.MODE_PIPES_AS_CONCAT);
        assertEquals(com.starrocks.sql.parser.SqlModeHelper.MODE_DEFAULT, com.starrocks.qe.SqlModeHelper.MODE_DEFAULT);
    }

    private static String sha256(Path file) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file));
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
