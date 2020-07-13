package com.examplesql.talend.components.output;

import org.talend.sdk.component.api.record.Schema;

import java.io.Serializable;

public class Column implements Serializable {

    private final Schema.Entry entry;

    private final boolean primaryKey;

    private final boolean sortKey;

    private final boolean distributionKey;

    private final Integer size;

    public Column(Schema.Entry entry, boolean primaryKey, boolean sortKey, boolean distributionKey, Integer size)
    {
        this.entry = entry;
        this.primaryKey = primaryKey;
        this.sortKey = sortKey;
        this.distributionKey = distributionKey;
        this.size = size;
    }

    public String getName() {
        return this.entry.getName();
    }

    public Schema.Entry getEntry() {
        return entry;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public boolean isSortKey() {
        return sortKey;
    }

    public boolean isDistributionKey() {
        return distributionKey;
    }

    public Integer getSize() {
        return size;
    }

    public static ColumnBuilder builder() {
        return new ColumnBuilder();
    }

    public static class ColumnBuilder {
        private Schema.Entry entry;
        private boolean primaryKey;
        private boolean sortKey;
        private boolean distributionKey;
        private Integer size;

        public ColumnBuilder entry(Schema.Entry entry)
        {
            this.entry = entry;
            return this;
        }

        public ColumnBuilder primaryKey(boolean primaryKey)
        {
            this.primaryKey = primaryKey;
            return this;
        }

        public ColumnBuilder sortKey(boolean sortKey)
        {
            this.sortKey = sortKey;
            return this;
        }

        public ColumnBuilder distributionKey(boolean distributionKey)
        {
            this.distributionKey = distributionKey;
            return this;
        }

        public ColumnBuilder size(Integer size) {
            this.size = size;
            return this;
        }

        public Column build() {
            return new Column(entry, primaryKey, sortKey, distributionKey, size);
        }
    }

}