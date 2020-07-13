package com.examplesql.talend.components.output;

import java.io.Serializable;
import java.util.List;

public class Table implements Serializable {

    private final String catalog;

    private final String schema;

    private final String name;

    private final List<Column> columns;

    private final List<Column> indexes;

    public Table(String catalog, String schema, String name, List<Column> columns, List<Column> indexes)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;
        this.columns = columns;
        this.indexes = indexes;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Column> getIndexes() {
        return indexes;
    }


    public static TableBuilder builder() {
        return new TableBuilder();
    }

    public static class TableBuilder {

        private String catalog;
        private String schema;
        private String name;
        private List<Column> columns;
        private List<Column> indexes;

        public TableBuilder catalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public TableBuilder schema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public TableBuilder name(String name)
        {
            this.name = name;
            return this;
        }

        public TableBuilder columns(List<Column> columns)
        {
            this.columns = columns;
            return this;
        }

        public TableBuilder indexes(List<Column> indexes)
        {
            this.indexes = indexes;
            return this;
        }

        public Table build() {
            return new Table(catalog, schema, name, columns, indexes);
        }
    }
}
