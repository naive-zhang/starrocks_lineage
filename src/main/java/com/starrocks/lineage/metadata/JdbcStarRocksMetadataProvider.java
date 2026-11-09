package com.starrocks.lineage.metadata;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.starrocks.lineage.model.Identifiers;
import com.starrocks.lineage.model.TableIdentifier;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;

/**
 * {@link MetadataProvider} reading StarRocks 3.1 metadata over JDBC (MySQL protocol), fronted by a Caffeine cache.
 *
 * <p><b>Lookup strategy</b></p>
 * <ul>
 *   <li>{@code default_catalog}: {@code information_schema.columns} with bind parameters, ordered by
 *       {@code ORDINAL_POSITION}. Works for OLAP tables, views and materialized views.</li>
 *   <li>External catalogs (Hive / Iceberg / Hudi / JDBC ...): {@code DESC `catalog`.`db`.`table`}, whose row
 *       order is the column order. StarRocks 3.1 supports three-part names in {@code DESC}.</li>
 * </ul>
 *
 * <p><b>Caching</b></p>
 * <ul>
 *   <li>Positive entries live {@code positiveTtl}; negative entries (table not found) live the much shorter
 *       {@code negativeTtl}, so a table created after the first miss becomes visible quickly while a burst of
 *       statements on a dropped table does not hammer the FE.</li>
 *   <li>Infrastructure errors are <b>not</b> cached: they surface as {@link MetadataAccessException}.</li>
 *   <li>Concurrent misses on the same key are coalesced by Caffeine (one query per key).</li>
 * </ul>
 */
public final class JdbcStarRocksMetadataProvider implements MetadataProvider {

    static final String INFORMATION_SCHEMA_SQL =
            "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE FROM information_schema.columns "
                    + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";

    private final DataSource dataSource;
    private final Config config;
    private final LoadingCache<TableIdentifier, Optional<TableSchema>> cache;

    public JdbcStarRocksMetadataProvider(DataSource dataSource) {
        this(dataSource, Config.defaults());
    }

    public JdbcStarRocksMetadataProvider(DataSource dataSource, Config config) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.config = Objects.requireNonNull(config, "config");
        this.cache = Caffeine.newBuilder()
                .maximumSize(config.maximumSize)
                .expireAfter(new PositiveNegativeExpiry(config.positiveTtl, config.negativeTtl))
                .recordStats()
                .build(this::load);
    }

    @Override
    public Optional<TableSchema> getTableSchema(TableIdentifier table) {
        Objects.requireNonNull(table, "table");
        try {
            return cache.get(table);
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof MetadataAccessException) {
                throw (MetadataAccessException) cause;
            }
            throw new MetadataAccessException("Failed to load metadata of " + table, cause == null ? e : cause);
        }
    }

    @Override
    public void invalidate(TableIdentifier table) {
        cache.invalidate(table);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    /** @return Caffeine statistics, exposed for monitoring */
    public com.github.benmanes.caffeine.cache.stats.CacheStats cacheStats() {
        return cache.stats();
    }

    private Optional<TableSchema> load(TableIdentifier table) {
        try (Connection conn = dataSource.getConnection()) {
            List<ColumnMetadata> columns = isDefaultCatalog(table)
                    ? loadFromInformationSchema(conn, table)
                    : loadFromDescribe(conn, table);
            return columns.isEmpty() ? Optional.empty() : Optional.of(new TableSchema(table, columns));
        } catch (SQLException e) {
            if (isNotFound(e)) {
                return Optional.empty();
            }
            throw new MetadataAccessException("Failed to load metadata of " + table + ": " + e.getMessage(), e);
        }
    }

    private boolean isDefaultCatalog(TableIdentifier table) {
        return Identifiers.DEFAULT_CATALOG.equals(table.getCatalog());
    }

    private List<ColumnMetadata> loadFromInformationSchema(Connection conn, TableIdentifier table)
            throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(INFORMATION_SCHEMA_SQL)) {
            ps.setQueryTimeout(config.queryTimeoutSeconds);
            ps.setString(1, table.getDatabase());
            ps.setString(2, table.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    String nullable = rs.getString(4);
                    columns.add(new ColumnMetadata(rs.getString(1), i++, rs.getString(3),
                            nullable == null || "YES".equalsIgnoreCase(nullable.trim())));
                }
            }
        }
        return columns;
    }

    private List<ColumnMetadata> loadFromDescribe(Connection conn, TableIdentifier table) throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        String sql = "DESC " + table.toQuotedSql();
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(config.queryTimeoutSeconds);
            try (ResultSet rs = st.executeQuery(sql)) {
                int i = 0;
                while (rs.next()) {
                    String nullable = rs.getString(3);
                    columns.add(new ColumnMetadata(rs.getString(1), i++, rs.getString(2),
                            nullable == null || "YES".equalsIgnoreCase(nullable.trim())));
                }
            }
        }
        return columns;
    }

    /**
     * Classifies "object does not exist" errors. StarRocks reports them with MySQL error codes 1049 (unknown
     * database), 1146 / 1051 (unknown table), SQLSTATE 42S02, or - for external catalogs - with a generic
     * code and a "not exist / not found" message.
     */
    static boolean isNotFound(SQLException e) {
        int code = e.getErrorCode();
        if (code == 1049 || code == 1146 || code == 1051) {
            return true;
        }
        if ("42S02".equals(e.getSQLState())) {
            return true;
        }
        String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return msg.contains("unknown table") || msg.contains("unknown database") || msg.contains("not exist")
                || msg.contains("not found") || msg.contains("unknown catalog");
    }

    /**
     * Variable expiry: different TTLs for present and absent schemas.
     */
    private static final class PositiveNegativeExpiry implements Expiry<TableIdentifier, Optional<TableSchema>> {
        private final long positiveNanos;
        private final long negativeNanos;

        PositiveNegativeExpiry(Duration positive, Duration negative) {
            this.positiveNanos = positive.toNanos();
            this.negativeNanos = negative.toNanos();
        }

        @Override
        public long expireAfterCreate(TableIdentifier key, Optional<TableSchema> value, long currentTime) {
            return value.isPresent() ? positiveNanos : negativeNanos;
        }

        @Override
        public long expireAfterUpdate(TableIdentifier key, Optional<TableSchema> value, long currentTime,
                                      long currentDuration) {
            return value.isPresent() ? positiveNanos : negativeNanos;
        }

        @Override
        public long expireAfterRead(TableIdentifier key, Optional<TableSchema> value, long currentTime,
                                    long currentDuration) {
            return currentDuration;
        }
    }

    /**
     * Immutable configuration.
     */
    public static final class Config {
        private final Duration positiveTtl;
        private final Duration negativeTtl;
        private final long maximumSize;
        private final int queryTimeoutSeconds;

        public Config(Duration positiveTtl, Duration negativeTtl, long maximumSize, int queryTimeoutSeconds) {
            this.positiveTtl = requirePositive(positiveTtl, "positiveTtl");
            this.negativeTtl = requirePositive(negativeTtl, "negativeTtl");
            if (maximumSize <= 0) {
                throw new IllegalArgumentException("maximumSize must be > 0");
            }
            if (queryTimeoutSeconds <= 0) {
                throw new IllegalArgumentException("queryTimeoutSeconds must be > 0");
            }
            this.maximumSize = maximumSize;
            this.queryTimeoutSeconds = queryTimeoutSeconds;
        }

        /** 10 min positive TTL, 1 min negative TTL, 50 000 tables, 10 s query timeout. */
        public static Config defaults() {
            return new Config(Duration.ofMinutes(10), Duration.ofMinutes(1), 50_000, 10);
        }

        private static Duration requirePositive(Duration d, String name) {
            Objects.requireNonNull(d, name);
            if (d.isNegative() || d.isZero()) {
                throw new IllegalArgumentException(name + " must be > 0");
            }
            return d;
        }

        public Duration getPositiveTtl() {
            return positiveTtl;
        }

        public Duration getNegativeTtl() {
            return negativeTtl;
        }

        public long getMaximumSize() {
            return maximumSize;
        }

        public int getQueryTimeoutSeconds() {
            return queryTimeoutSeconds;
        }
    }
}
