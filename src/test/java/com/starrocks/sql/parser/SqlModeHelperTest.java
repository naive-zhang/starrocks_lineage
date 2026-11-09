package com.starrocks.sql.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlModeHelperTest {

    @Test
    void encodeAndDecodeRoundTrip() {
        long mode = SqlModeHelper.encode(" pipes_as_concat , ONLY_FULL_GROUP_BY,");
        assertEquals(SqlModeHelper.MODE_PIPES_AS_CONCAT | SqlModeHelper.MODE_ONLY_FULL_GROUP_BY, mode);
        assertEquals("PIPES_AS_CONCAT,ONLY_FULL_GROUP_BY", SqlModeHelper.decode(mode));
        assertTrue(SqlModeHelper.isPipesAsConcat(mode));
        assertFalse(SqlModeHelper.isPipesAsConcat(SqlModeHelper.MODE_DEFAULT));
        assertEquals(0L, SqlModeHelper.encode(null));
        assertEquals(0L, SqlModeHelper.encode("  "));
    }

    @Test
    void unknownModeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> SqlModeHelper.encode("PIPES_AS_CONCATT"));
    }

    @Test
    void qeShimDelegatesToCanonicalHelper() {
        assertEquals(SqlModeHelper.encode("ANSI_QUOTES"), com.starrocks.qe.SqlModeHelper.encode("ANSI_QUOTES"));
        assertEquals("SORT_NULLS_LAST", com.starrocks.qe.SqlModeHelper.decode(
                com.starrocks.qe.SqlModeHelper.MODE_SORT_NULLS_LAST));
    }
}
