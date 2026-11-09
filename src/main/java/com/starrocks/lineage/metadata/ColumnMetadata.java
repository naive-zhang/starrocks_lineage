package com.starrocks.lineage.metadata;

import com.starrocks.lineage.model.Identifiers;

import java.util.Objects;

/**
 * Column of a table schema. {@code ordinal} is 0-based and defines the order used for {@code SELECT *}
 * expansion and for {@code INSERT INTO t SELECT ...} positional alignment.
 */
public final class ColumnMetadata {

    private final String name;
    private final int ordinal;
    private final String dataType;
    private final boolean nullable;

    public ColumnMetadata(String name, int ordinal, String dataType, boolean nullable) {
        String n = Identifiers.normalizeColumn(name);
        if (n == null || n.isEmpty()) {
            throw new IllegalArgumentException("column name must not be empty");
        }
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal must be >= 0: " + ordinal);
        }
        this.name = n;
        this.ordinal = ordinal;
        this.dataType = dataType;
        this.nullable = nullable;
    }

    /** @return normalised (lower-case) column name */
    public String getName() {
        return name;
    }

    public int getOrdinal() {
        return ordinal;
    }

    /** @return data type as reported by StarRocks, may be {@code null} when unknown */
    public String getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnMetadata)) {
            return false;
        }
        ColumnMetadata that = (ColumnMetadata) o;
        return ordinal == that.ordinal && nullable == that.nullable && name.equals(that.name)
                && Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ordinal, dataType, nullable);
    }

    @Override
    public String toString() {
        return name + (dataType == null ? "" : " " + dataType);
    }
}
