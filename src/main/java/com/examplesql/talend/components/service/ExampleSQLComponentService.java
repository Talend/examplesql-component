package com.examplesql.talend.components.service;

import com.examplesql.talend.components.dataset.TableNameDataset;
import com.examplesql.talend.components.datastore.ExampleSQLDatastore;
import com.examplesql.talend.components.output.Column;
import com.examplesql.talend.components.output.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.*;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

@Service
public class ExampleSQLComponentService {

    private static final transient Logger LOG = LoggerFactory.getLogger(ExampleSQLComponentService.class);

    // you can put logic here you can reuse in components
    private static Pattern READ_ONLY_QUERY_PATTERN = Pattern.compile(
            "^SELECT\\s+((?!((\\bINTO\\b)|(\\bFOR\\s+UPDATE\\b)|(\\bLOCK\\s+IN\\s+SHARE\\s+MODE\\b))).)+$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    @Service
    private I18nMessage i18n;

    public boolean isNotReadOnlySQLQuery(final String query) {
        return query != null && !READ_ONLY_QUERY_PATTERN.matcher(query.trim()).matches();
    }

    public static boolean checkTableExistence(final String tableName, final ExampleSQLComponentService.DataSource dataSource)
            throws SQLException {
        try (final Connection connection = dataSource.getConnection()) {
            try (final ResultSet resultSet = connection.getMetaData().getTables(connection.getCatalog(), connection.getSchema(),
                    tableName, new String[] { "TABLE", "SYNONYM" })) {
                while (resultSet.next()) {
                    if (ofNullable(ofNullable(resultSet.getString("TABLE_NAME")).orElseGet(() -> {
                        try {
                            return resultSet.getString("SYNONYM_NAME");
                        } catch (final SQLException e) {
                            return null;
                        }
                    })).filter(tableName::equals).isPresent()) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    public void createTableIfNotExist(final Connection connection, final String name, final List<String> keys,
                                       final int varcharLength, final List<Record> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        final String sql = buildQuery(getTableModel(connection, name, keys, varcharLength, records));
        try (final Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            connection.commit();
        } catch (final Throwable e) {
            connection.rollback();
            if (!isTableExistsCreationError(e)) {
                throw new IllegalStateException(e);
            }

            LOG.trace("create table issue was ignored. The table and it's name space has been created by an other worker", e);
        }
    }

    protected Table getTableModel(final Connection connection, final String name, final List<String> keys,
                                  final int varcharLength, final List<Record> records) {
        final Table.TableBuilder builder = Table.builder().name(name);
        try {
            builder.catalog(connection.getCatalog()).schema(connection.getSchema());
        } catch (final SQLException e) {
            LOG.warn("can't get database catalog or schema", e);
        }
        final List<Schema.Entry> entries = records.stream().flatMap(record -> record.getSchema().getEntries().stream()).distinct()
                .collect(toList());
        return builder.columns(entries.stream()
                .map(entry -> Column.builder().entry(entry).primaryKey(keys.contains(entry.getName()))
                        .size(STRING == entry.getType() ? varcharLength : null).build())
                .collect(toList())).build();
    }

    protected String buildQuery(final Table table) {
        // keep the string builder for readability
        final StringBuilder sql = new StringBuilder("CREATE TABLE");
        sql.append(" ");
        sql.append("IF NOT EXISTS");
        sql.append(" ");
        if (table.getSchema() != null && !table.getSchema().isEmpty()) {
            sql.append(identifier(table.getSchema())).append(".");
        }
        sql.append(identifier(table.getName()));
        sql.append("(");
        sql.append(createColumns(table.getColumns()));
        sql.append(createPKs(table.getName(),
                table.getColumns().stream().filter(Column::isPrimaryKey).collect(Collectors.toList())));
        sql.append(")");
        // todo create index

        LOG.debug("### create table query ###");
        LOG.debug(sql.toString());
        return sql.toString();
    }

    protected boolean isTableExistsCreationError(Throwable e) {
        return false;
    }

    @Suggestions("actionsList")
    public SuggestionValues getActionOnData(@Option final TableNameDataset dataset) {
        SuggestionValues suggestionValues = new SuggestionValues();
        List<SuggestionValues.Item> items = new ArrayList<SuggestionValues.Item>();
        items.add(new SuggestionValues.Item("INSERT","INSERT"));
        items.add(new SuggestionValues.Item("UPDATE","UPDATE"));
        items.add(new SuggestionValues.Item("UPSERT","UPSERT"));
        items.add(new SuggestionValues.Item("DELETE","DELETE"));
        items.add(new SuggestionValues.Item("BULK_LOAD","BULK_LOAD"));

        suggestionValues.setItems(items);

        return suggestionValues;
    }

    @Suggestions("suggestTableColumnNames")
    public SuggestionValues getTableColumns(@Option final TableNameDataset dataset) {
        try (Connection conn = createDataSource(dataset.getDatastore()).getConnection()) {
            try (final Statement statement = conn.createStatement()) {
                statement.setMaxRows(1);
                try (final ResultSet result = statement.executeQuery("select * from " + dataset.getTableName())) {
                    return new SuggestionValues(true,
                            IntStream.rangeClosed(1, result.getMetaData().getColumnCount()).mapToObj(i -> {
                                try {
                                    return result.getMetaData().getColumnName(i);
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }

                                return null;
                            }).filter(Objects::nonNull).map(columnName -> new SuggestionValues.Item(columnName, columnName))
                                    .collect(toSet()));
                }
            }
        } catch (final Exception unexpected) {
            // catch all exceptions for this ui label to return empty list
            LOG.error(i18n.errorCantLoadTableSuggestions(), unexpected);
        }

        return new SuggestionValues(false, emptyList());
    }

    @Suggestions("listTables")
    public SuggestionValues getTableFromDatabase(@Option final ExampleSQLDatastore datastore) {
        final Collection<SuggestionValues.Item> items = new HashSet<>();
        try (Connection connection = createDataSource(datastore).getConnection()) {
            final DatabaseMetaData dbMetaData = connection.getMetaData();
            try (ResultSet tables = dbMetaData.getTables(connection.getCatalog(), connection.getSchema(), null,
                    getAvailableTableTypes(dbMetaData).toArray(new String[0]))) {
                while (tables.next()) {
                    ofNullable(ofNullable(tables.getString("TABLE_NAME")).orElseGet(() -> {
                        try {
                            return tables.getString("SYNONYM_NAME");
                        } catch (final SQLException e) {
                            return null;
                        }
                    })).ifPresent(t -> items.add(new SuggestionValues.Item(t, t)));
                }
            }
        } catch (final Exception unexpected) { // catch all exceptions for this ui label to return empty list
            LOG.error(i18n.errorCantLoadTableSuggestions(), unexpected);
        }
        return new SuggestionValues(true, items);
    }

    private Set<String> getSupportedTableTypes()
    {
        Set<String> tableTypes = new HashSet<String>();
        tableTypes.add("TABLE");
        tableTypes.add("VIEW");
        tableTypes.add("SYNONYM");

        return tableTypes;
    }

    private Set<String> getAvailableTableTypes(DatabaseMetaData dbMetaData) throws SQLException {
        Set<String> result = new HashSet<>();
        try (ResultSet tables = dbMetaData.getTableTypes()) {
            while (tables.next()) {
                ofNullable(tables.getString("TABLE_TYPE")).map(String::trim)
                        .map(t -> ("BASE TABLE".equalsIgnoreCase(t)) ? "TABLE" : t)
                        .filter(t -> getSupportedTableTypes().contains(t)).ifPresent(result::add);
            }
        }
        return result;
    }

    @HealthCheck("validateConnection")
    public HealthCheckStatus validateConnection(@Option final ExampleSQLDatastore datastore)
    {
        DataSource dataSource = createDataSource(datastore);
        try {
            dataSource.createConnection();
            dataSource.testConnection();
            return new HealthCheckStatus(HealthCheckStatus.Status.OK,i18n.successConnection());
        } catch(SQLException e)
        {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO,i18n.errorInvalidConnection());
        }
    }




    public DataSource createDataSource(final ExampleSQLDatastore connection)
    {

        return new DataSource(i18n, connection, false, false);
    }

    public DataSource createDataSource(final ExampleSQLDatastore connection, final boolean rewriteBatchedStatements) {
        return new DataSource(i18n, connection, false, rewriteBatchedStatements);
    }

    public DataSource createDataSource(final ExampleSQLDatastore connection, boolean isAutoCommit,
                                           final boolean rewriteBatchedStatements) {

        return new DataSource(i18n, connection, isAutoCommit, rewriteBatchedStatements);
    }

    public static class DataSource implements AutoCloseable
    {
        private Connection connection;
        private final I18nMessage i18nMessage;
        private final ExampleSQLDatastore datastore;
        private final boolean isAutoCommit;
        private final boolean rewriteBatchedStatements;
        public DataSource(final I18nMessage i18nMessage, final ExampleSQLDatastore datastore,
                          final boolean isAutoCommit, final boolean rewriteBatchedStatements)
        {
            this.i18nMessage = i18nMessage;
            this.datastore = datastore;
            this.isAutoCommit = isAutoCommit;
            this.rewriteBatchedStatements = rewriteBatchedStatements;
        }

        private void createConnection()
        {
            try {
                Class.forName("org.mariadb.jdbc.Driver");
                connection = DriverManager.getConnection(datastore.getJdbcUrl()+ "?rewriteBatchedStatements=" + String.valueOf(rewriteBatchedStatements),
                        datastore.getUser(), datastore.getPassword());
                connection.setAutoCommit(isAutoCommit);

            } catch (ClassNotFoundException | SQLException e) {
                LOG.error(e.getMessage());
            }
        }

        public Connection getConnection() {
            try {
                if (connection == null || connection.isClosed())
                    createConnection();
            } catch (SQLException e) {
                LOG.error(e.getMessage());
            }

            return this.connection;
        }

        public void testConnection() throws SQLException {
            connection.isClosed();
        }

        @Override
        public void close() throws Exception {
            connection.close();
        }
    }

    @AsyncValidation("validateQuery")
    public ValidationResult validateReadOnlySQLQuery(final String query) {
        if (isNotReadOnlySQLQuery(query)) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.errorUnauthorizedQuery());
        }
        return new ValidationResult(ValidationResult.Status.OK, "the query is valid");
    }

    public String identifier(final String name) {
        return name == null || name.isEmpty() ? name : delimiterToken() + name + delimiterToken();
    }

    protected String delimiterToken() {
        return "`";
    }

    private String createColumns(final List<Column> columns) {
        return columns.stream().map(this::createColumn).collect(joining(","));
    }

    private String createColumn(final Column column) {
        return identifier(column.getEntry().getName())//
                + " " + toDBType(column)//
                + " " + isRequired(column)//
                ;
    }

    private String toDBType(final Column column) {
        switch (column.getEntry().getType()) {
            case STRING:
                return column.getSize() <= -1 ? (column.isPrimaryKey() ? "VARCHAR(255)" : "TEXT")
                        : "VARCHAR(" + column.getSize() + ")";
            case BOOLEAN:
                return "BOOLEAN";
            case DOUBLE:
                return "DOUBLE";
            case FLOAT:
                return "FLOAT";
            case LONG:
                return "BIGINT";
            case INT:
                return "INT";
            case BYTES:
                return "BLOB";
            case DATETIME:
                return "DATETIME(6)";
            case RECORD:
            case ARRAY:
            default:
                throw new IllegalStateException(this.i18n.errorUnsupportedType(column.getEntry().getType().name(), column.getEntry().getName()));
        }
    }

    private String isRequired(final Column column) {
        return column.getEntry().isNullable() && !column.isPrimaryKey() ? "NULL" : "NOT NULL";
    }

    private String createPKs(final String table, final List<Column> primaryKeys) {
        return primaryKeys == null || primaryKeys.isEmpty() ? ""
                : ", CONSTRAINT " + pkConstraintName(table, primaryKeys) + " PRIMARY KEY "
                + primaryKeys.stream().map(Column::getName).map(this::identifier).collect(joining(",", "(", ")"));
    }

    private String pkConstraintName(String table, List<Column> primaryKeys) {
        final String uuid = UUID.randomUUID().toString();
        return "pk_" + table + "_" + primaryKeys.stream().map(Column::getName).collect(joining("_")) + "_"
                + uuid.substring(0, Math.min(4, uuid.length()));
    }
}