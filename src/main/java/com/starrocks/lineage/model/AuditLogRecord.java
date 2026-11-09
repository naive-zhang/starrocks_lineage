package com.starrocks.lineage.model;

import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Objects;

/**
 * One row of {@code starrocks_audit_db__.starrocks_audit_tbl__} restricted to the columns this engine reads.
 * Immutable; build with {@link #builder()}.
 */
public final class AuditLogRecord {

    private final String queryId;
    private final LocalDateTime timestamp;
    private final String queryType;
    private final String clientIp;
    private final String user;
    private final String authorizedUser;
    private final String catalog;
    private final String db;
    private final String state;
    private final String errorCode;
    private final long queryTime;
    private final long scanBytes;
    private final long scanRows;
    private final long returnRows;
    private final long cpuCostNs;
    private final long memCostBytes;
    private final boolean isQuery;
    private final String stmt;
    private final String digest;

    private AuditLogRecord(Builder b) {
        this.queryId = Objects.requireNonNull(b.queryId, "queryId");
        this.timestamp = Objects.requireNonNull(b.timestamp, "timestamp");
        this.queryType = b.queryType;
        this.clientIp = b.clientIp;
        this.user = b.user;
        this.authorizedUser = b.authorizedUser;
        this.catalog = b.catalog;
        this.db = b.db;
        this.state = b.state;
        this.errorCode = b.errorCode;
        this.queryTime = b.queryTime;
        this.scanBytes = b.scanBytes;
        this.scanRows = b.scanRows;
        this.returnRows = b.returnRows;
        this.cpuCostNs = b.cpuCostNs;
        this.memCostBytes = b.memCostBytes;
        this.isQuery = b.isQuery;
        this.stmt = b.stmt;
        this.digest = b.digest;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Pre-filter rule: a statement is failed when {@code state = 'ERR'} (case-insensitive) or when
     * {@code errorCode} is non-empty and not {@code "0"}. In StarRocks 3.1 {@code errorCode} is a VARCHAR that
     * may contain either a numeric code or an error name, so a textual comparison is used.
     */
    public boolean isFailed() {
        if (state != null && "ERR".equals(state.trim().toUpperCase(Locale.ROOT))) {
            return true;
        }
        if (errorCode == null) {
            return false;
        }
        String code = errorCode.trim();
        return !code.isEmpty() && !"0".equals(code);
    }

    /** @return the namespace in which the statement ran */
    public NamespaceContext namespace() {
        return NamespaceContext.fromAudit(catalog, db);
    }

    /**
     * @return user name used for UV metrics: {@code user}, falling back to {@code authorizedUser}
     */
    public String effectiveUser() {
        if (user != null && !user.trim().isEmpty()) {
            return user.trim();
        }
        return authorizedUser == null ? "" : authorizedUser.trim();
    }

    public String getQueryId() {
        return queryId;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String getQueryType() {
        return queryType;
    }

    public String getClientIp() {
        return clientIp;
    }

    public String getUser() {
        return user;
    }

    public String getAuthorizedUser() {
        return authorizedUser;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDb() {
        return db;
    }

    public String getState() {
        return state;
    }

    public String getErrorCode() {
        return errorCode;
    }

    /** @return query time in milliseconds */
    public long getQueryTime() {
        return queryTime;
    }

    public long getScanBytes() {
        return scanBytes;
    }

    public long getScanRows() {
        return scanRows;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public long getCpuCostNs() {
        return cpuCostNs;
    }

    public long getMemCostBytes() {
        return memCostBytes;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public String getStmt() {
        return stmt;
    }

    public String getDigest() {
        return digest;
    }

    @Override
    public String toString() {
        return "AuditLogRecord{queryId=" + queryId + ", timestamp=" + timestamp + ", user=" + user + ", state="
                + state + '}';
    }

    /**
     * Builder for {@link AuditLogRecord}.
     */
    public static final class Builder {
        private String queryId;
        private LocalDateTime timestamp;
        private String queryType;
        private String clientIp;
        private String user;
        private String authorizedUser;
        private String catalog;
        private String db;
        private String state;
        private String errorCode;
        private long queryTime;
        private long scanBytes;
        private long scanRows;
        private long returnRows;
        private long cpuCostNs;
        private long memCostBytes;
        private boolean isQuery;
        private String stmt;
        private String digest;

        private Builder() {
        }

        public Builder queryId(String v) {
            this.queryId = v;
            return this;
        }

        public Builder timestamp(LocalDateTime v) {
            this.timestamp = v;
            return this;
        }

        public Builder queryType(String v) {
            this.queryType = v;
            return this;
        }

        public Builder clientIp(String v) {
            this.clientIp = v;
            return this;
        }

        public Builder user(String v) {
            this.user = v;
            return this;
        }

        public Builder authorizedUser(String v) {
            this.authorizedUser = v;
            return this;
        }

        public Builder catalog(String v) {
            this.catalog = v;
            return this;
        }

        public Builder db(String v) {
            this.db = v;
            return this;
        }

        public Builder state(String v) {
            this.state = v;
            return this;
        }

        public Builder errorCode(String v) {
            this.errorCode = v;
            return this;
        }

        public Builder queryTime(long v) {
            this.queryTime = v;
            return this;
        }

        public Builder scanBytes(long v) {
            this.scanBytes = v;
            return this;
        }

        public Builder scanRows(long v) {
            this.scanRows = v;
            return this;
        }

        public Builder returnRows(long v) {
            this.returnRows = v;
            return this;
        }

        public Builder cpuCostNs(long v) {
            this.cpuCostNs = v;
            return this;
        }

        public Builder memCostBytes(long v) {
            this.memCostBytes = v;
            return this;
        }

        public Builder isQuery(boolean v) {
            this.isQuery = v;
            return this;
        }

        public Builder stmt(String v) {
            this.stmt = v;
            return this;
        }

        public Builder digest(String v) {
            this.digest = v;
            return this;
        }

        public AuditLogRecord build() {
            return new AuditLogRecord(this);
        }
    }
}
