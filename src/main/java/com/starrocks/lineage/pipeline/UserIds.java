package com.starrocks.lineage.pipeline;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Stateless user ids for {@code user_bitmap}: the first 63 bits of SHA-256 of the user name.
 *
 * <p>A shared counter (max id + 1) needs a single writer; two instances racing - or a paused instance resuming after a
 * take-over - can hand the same id to two users and silently merge them in the exact UV. A content-derived id is the
 * same on every instance and needs no coordination. The probability that two of one million users collide is about
 * 5e-8; {@link AuditLineageSink#userIdCollisions()} detects it.</p>
 */
public final class UserIds {

    private UserIds() {
    }

    /** @return a positive id in {@code 1 .. 2^63-1}, a pure function of {@code user} */
    public static long idOf(String user) {
        Objects.requireNonNull(user, "user");
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(user.getBytes(StandardCharsets.UTF_8));
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v = (v << 8) | (d[i] & 0xFF);
            }
            v &= Long.MAX_VALUE;
            return v == 0 ? 1 : v;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
