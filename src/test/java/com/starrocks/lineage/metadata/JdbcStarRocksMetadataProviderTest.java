package com.starrocks.lineage.metadata;

import com.starrocks.lineage.model.TableIdentifier;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the JDBC provider against H2, whose {@code INFORMATION_SCHEMA.COLUMNS} exposes the same columns
 * ({@code TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE}) StarRocks does.
 */
class JdbcStarRocksMetadataProviderTest {

    private Connection keepAlive;
    private CountingDataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:meta_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        keepAlive = h2.getConnection();
        try (Statement st = keepAlive.createStatement()) {
            st.execute("CREATE SCHEMA \"ods\"");
            st.execute("CREATE TABLE \"ods\".\"Orders\" (\"order_id\" BIGINT NOT NULL, \"User_Id\" INT, "
                    + "\"amount\" DECIMAL(18,2))");
        }
        dataSource = new CountingDataSource(h2);
    }

    @AfterEach
    void tearDown() throws Exception {
        keepAlive.close();
    }

    @Test
    void loadsOrderedColumnsFromInformationSchema() {
        JdbcStarRocksMetadataProvider p = new JdbcStarRocksMetadataProvider(dataSource);
        TableSchema schema = p.getTableSchema(TableIdentifier.parse("default_catalog.ods.Orders")).orElseThrow();
        assertEquals(Arrays.asList("order_id", "user_id", "amount"), schema.getColumnNames());
        assertFalse(schema.getColumns().get(0).isNullable());
        assertTrue(schema.getColumns().get(1).isNullable());
        assertEquals("BIGINT", schema.getColumns().get(0).getDataType());
    }

    @Test
    void tableNamesAreCaseSensitive() {
        JdbcStarRocksMetadataProvider p = new JdbcStarRocksMetadataProvider(dataSource);
        assertFalse(p.tableExists(TableIdentifier.parse("default_catalog.ods.orders")));
    }

    @Test
    void positiveAndNegativeResultsAreCachedAndCanBeInvalidated() throws Exception {
        JdbcStarRocksMetadataProvider p = new JdbcStarRocksMetadataProvider(dataSource);
        TableIdentifier existing = TableIdentifier.parse("default_catalog.ods.Orders");
        TableIdentifier later = TableIdentifier.parse("default_catalog.ods.late");

        for (int i = 0; i < 5; i++) {
            assertTrue(p.tableExists(existing));
            assertFalse(p.tableExists(later));
        }
        assertEquals(2, dataSource.connections.get(), "one load per key, then served from cache");

        try (Statement st = keepAlive.createStatement()) {
            st.execute("CREATE TABLE \"ods\".\"late\" (\"k\" INT)");
        }
        assertFalse(p.tableExists(later), "negative entry still cached");
        p.invalidate(later);
        assertTrue(p.tableExists(later));
        assertEquals(3, dataSource.connections.get());
    }

    @Test
    void negativeEntriesExpireFasterThanPositiveOnes() throws Exception {
        JdbcStarRocksMetadataProvider p = new JdbcStarRocksMetadataProvider(dataSource,
                new JdbcStarRocksMetadataProvider.Config(Duration.ofHours(1), Duration.ofMillis(50), 100, 5));
        TableIdentifier later = TableIdentifier.parse("default_catalog.ods.late2");
        assertFalse(p.tableExists(later));
        try (Statement st = keepAlive.createStatement()) {
            st.execute("CREATE TABLE \"ods\".\"late2\" (\"k\" INT)");
        }
        long deadline = System.currentTimeMillis() + 5_000;
        boolean visible = false;
        while (!visible && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
            visible = p.tableExists(later);
        }
        assertTrue(visible, "negative TTL elapsed, table must become visible");
    }

    @Test
    void infrastructureErrorsAreRaisedAndNotCached() {
        dataSource.failNext.set(true);
        JdbcStarRocksMetadataProvider p = new JdbcStarRocksMetadataProvider(dataSource);
        TableIdentifier t = TableIdentifier.parse("default_catalog.ods.Orders");
        MetadataAccessException e = assertThrows(MetadataAccessException.class, () -> p.getTableSchema(t));
        assertTrue(e.getMessage().contains("connection refused"));
        assertTrue(p.tableExists(t), "the failure must not have been cached");
    }

    @Test
    void externalCatalogUsesThreePartDescribe() {
        List<String> executed = Collections.synchronizedList(new ArrayList<>());
        DataSource describing = new DescribeEmulatingDataSource(dataSource, executed);
        JdbcStarRocksMetadataProvider p = new JdbcStarRocksMetadataProvider(describing);
        TableSchema schema = p.getTableSchema(TableIdentifier.parse("Hive.`ods`.`odd``name`")).orElseThrow();
        assertEquals(Collections.singletonList("DESC `hive`.`ods`.`odd``name`"), executed);
        assertEquals(Arrays.asList("id", "name", "tags"), schema.getColumnNames());
        assertEquals("ARRAY<STRING>", schema.getColumns().get(2).getDataType());
        assertFalse(schema.getColumns().get(1).isNullable());
    }

    @Test
    void notFoundClassification() {
        assertTrue(JdbcStarRocksMetadataProvider.isNotFound(new SQLException("x", "HY000", 1146)));
        assertTrue(JdbcStarRocksMetadataProvider.isNotFound(new SQLException("x", "HY000", 1049)));
        assertTrue(JdbcStarRocksMetadataProvider.isNotFound(new SQLException("x", "42S02", 0)));
        assertTrue(JdbcStarRocksMetadataProvider.isNotFound(
                new SQLException("Table t not found in hive catalog", "HY000", 1064)));
        assertFalse(JdbcStarRocksMetadataProvider.isNotFound(
                new SQLException("Access denied for user", "28000", 1045)));
    }

    @Test
    void configValidation() {
        assertThrows(IllegalArgumentException.class, () -> new JdbcStarRocksMetadataProvider.Config(
                Duration.ZERO, Duration.ofSeconds(1), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new JdbcStarRocksMetadataProvider.Config(
                Duration.ofSeconds(1), Duration.ofSeconds(1), 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new JdbcStarRocksMetadataProvider.Config(
                Duration.ofSeconds(1), Duration.ofSeconds(1), 1, 0));
    }

    /** Counts connections and can simulate one infrastructure failure. */
    private static final class CountingDataSource extends DelegatingDataSource {
        final AtomicInteger connections = new AtomicInteger();
        final AtomicBoolean failNext = new AtomicBoolean();

        CountingDataSource(DataSource delegate) {
            super(delegate);
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (failNext.compareAndSet(true, false)) {
                throw new SQLException("connection refused", "08001", 0);
            }
            connections.incrementAndGet();
            return delegate.getConnection();
        }
    }

    /**
     * H2 has no {@code DESC}; emulate StarRocks' answer (Field, Type, Null, Key, Default, Extra) while recording
     * the SQL text actually issued by the provider.
     */
    private static final class DescribeEmulatingDataSource extends DelegatingDataSource {
        private final List<String> executed;

        DescribeEmulatingDataSource(DataSource delegate, List<String> executed) {
            super(delegate);
            this.executed = executed;
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection real = delegate.getConnection();
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, args) -> {
                        if ("createStatement".equals(method.getName()) && (args == null || args.length == 0)) {
                            Statement realStatement = real.createStatement();
                            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Statement.class},
                                    (p2, m2, a2) -> {
                                        if ("executeQuery".equals(m2.getName())) {
                                            executed.add((String) a2[0]);
                                            return realStatement.executeQuery(
                                                    "SELECT * FROM (VALUES ('Id', 'INT', 'YES', 'true', NULL, ''),"
                                                            + " ('Name', 'VARCHAR(64)', 'NO', 'false', NULL, ''),"
                                                            + " ('tags', 'ARRAY<STRING>', 'YES', 'false', NULL, ''))"
                                                            + " AS d(f, t, n, k, df, e)");
                                        }
                                        return invoke(realStatement, m2, a2);
                                    });
                        }
                        return invoke(real, method, args);
                    });
        }

        private static Object invoke(Object target, java.lang.reflect.Method m, Object[] args) throws Throwable {
            try {
                return m.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }

    private abstract static class DelegatingDataSource implements DataSource {
        final DataSource delegate;

        DelegatingDataSource(DataSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            delegate.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }
}
