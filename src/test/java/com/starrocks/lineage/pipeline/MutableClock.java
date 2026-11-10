package com.starrocks.lineage.pipeline;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * A clock tests can move (UTC; {@link LocalDateTime#now(Clock)} returns the set local time).
 */
final class MutableClock extends Clock {

    private volatile Instant instant;

    MutableClock(LocalDateTime start) {
        this.instant = start.toInstant(ZoneOffset.UTC);
    }

    void set(LocalDateTime t) {
        instant = t.toInstant(ZoneOffset.UTC);
    }

    void advance(Duration d) {
        instant = instant.plus(d);
    }

    LocalDateTime now() {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
        return instant;
    }
}
