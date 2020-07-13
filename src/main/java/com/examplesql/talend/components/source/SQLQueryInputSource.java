package com.examplesql.talend.components.source;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.examplesql.talend.components.service.I18nMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;


import com.examplesql.talend.components.service.ExampleSQLComponentService;

import static java.sql.ResultSetMetaData.columnNoNulls;
import static org.talend.sdk.component.api.record.Schema.Type.*;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

@Documentation("TODO fill the documentation for this source")
public class SQLQueryInputSource implements Serializable {
    private final SQLQueryInputMapperConfiguration configuration;
    private final ExampleSQLComponentService service;
    private final RecordBuilderFactory builderFactory;
    private final I18nMessage i18n;

    private ExampleSQLComponentService.DataSource dataSource;
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private transient Schema schema;
    private boolean isGuessSchema = false;
    private static final transient Logger LOG = LoggerFactory.getLogger(SQLQueryInputSource.class);

    public SQLQueryInputSource(@Option("configuration") final SQLQueryInputMapperConfiguration configuration,
                                final ExampleSQLComponentService service,
                                final RecordBuilderFactory builderFactory,
                               final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        // this method will be executed once for the whole component execution,
        // this is where you can establish a connection for instance
        if (configuration.getDataset().getQuery() == null || configuration.getDataset().getQuery().trim().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyQuery());
        }
        if (service.isNotReadOnlySQLQuery(configuration.getDataset().getQuery())) {
            throw new IllegalArgumentException(i18n.errorUnauthorizedQuery());
        }

        try {
            dataSource = service.createDataSource(configuration.getDataset().getDatastore());
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.setFetchSize(configuration.getDataset().getFetchSize());
            resultSet = statement.executeQuery(configuration.getDataset().getQuery());
            StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
            String genisisClass = stackTraceElements[stackTraceElements.length -1].getClassName();
            if (genisisClass.contains("guess_schema"))
            {

                isGuessSchema = true;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Producer
    public Record next() {
        // this is the method allowing you to go through the dataset associated
        // to the component configuration
        //
        // return null means the dataset has no more data to go through
        // you can use the builderFactory to create a new Record.
        try {
            if (!resultSet.next()  && !isGuessSchema) {
                return null;
            }

            final ResultSetMetaData metaData = resultSet.getMetaData();
            if (schema == null) {
                final Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(RECORD);
                IntStream.rangeClosed(1, metaData.getColumnCount()).forEach(index -> addField(schemaBuilder, metaData, index));
                schema = schemaBuilder.build();
            }

            final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schema);
            if (isGuessSchema) {
                IntStream.rangeClosed(1, metaData.getColumnCount()).forEach(index -> emptyTable(recordBuilder, metaData, index));
                isGuessSchema = false;
            }
            else
                IntStream.rangeClosed(1, metaData.getColumnCount()).forEach(index -> addColumn(recordBuilder, metaData, index));
            return recordBuilder.build();
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
        if (resultSet != null ) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.warn(i18n.warnResultSetCantBeClosed(), e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.warn(i18n.warnStatementCantBeClosed(), e);
            }
        }
        if (connection != null) {
            try {
                connection.commit();
            } catch (final SQLException e) {
                LOG.error(i18n.errorSQL(e.getErrorCode(), e.getMessage()), e);
                try {
                    connection.rollback();
                } catch (final SQLException rollbackError) {
                    LOG.error(i18n.errorSQL(rollbackError.getErrorCode(), rollbackError.getMessage()), rollbackError);
                }
            }
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn(i18n.warnConnectionCantBeClosed(), e);
            }
        }
        if (dataSource != null) {
            try {
                dataSource.close();
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }

    private void addField(final Schema.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final int sqlType = metaData.getColumnType(columnIndex);
            final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != columnNoNulls);
            switch (sqlType) {
                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                    if (javaType.equals(Integer.class.getName())) {
                        builder.withEntry(entryBuilder.withType(INT).build());
                    } else {
                        builder.withEntry(entryBuilder.withType(LONG).build());
                    }
                    break;
                case Types.FLOAT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.REAL:
                    builder.withEntry(entryBuilder.withType(FLOAT).build());
                    break;
                case Types.DOUBLE:
                    builder.withEntry(entryBuilder.withType(DOUBLE).build());
                    break;
                case Types.BOOLEAN:
                    builder.withEntry(entryBuilder.withType(BOOLEAN).build());
                    break;
                case Types.TIME:
                case Types.DATE:
                case Types.TIMESTAMP:
                    builder.withEntry(entryBuilder.withType(DATETIME).build());
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    builder.withEntry(entryBuilder.withType(BYTES).build());
                    break;
                case Types.BIGINT:
                    builder.withEntry(entryBuilder.withType(LONG).build());
                    break;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                default:
                    builder.withEntry(entryBuilder.withType(STRING).build());
                    break;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void addColumn(final Record.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final int sqlType = metaData.getColumnType(columnIndex);
            final Object value = resultSet.getObject(columnIndex);
            final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != columnNoNulls);
            switch (sqlType) {
                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                    if (value != null) {
                        if (javaType.equals(Integer.class.getName())) {
                            builder.withInt(entryBuilder.withType(INT).build(), (Integer) value);
                        } else {
                            builder.withLong(entryBuilder.withType(LONG).build(), (Long) value);
                        }
                    }
                    break;
                case Types.FLOAT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.REAL:
                    if (value != null) {
                        builder.withFloat(entryBuilder.withType(FLOAT).build(), javaType.equals("java.math.BigDecimal") ? ((BigDecimal) value).floatValue() : (Float) value);
                    }
                    break;
                case Types.DOUBLE:
                    if (value != null) {
                        builder.withDouble(entryBuilder.withType(DOUBLE).build(), (Double) value);
                    }
                    break;
                case Types.BOOLEAN:
                    if (value != null) {
                        builder.withBoolean(entryBuilder.withType(BOOLEAN).build(), (Boolean) value);
                    }
                    break;
                case Types.DATE:
                    builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Date) value).getTime()));
                    break;
                case Types.TIME:
                    builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Time) value).getTime()));
                    break;
                case Types.TIMESTAMP:
                    builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Timestamp) value).getTime()));
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    builder.withBytes(entryBuilder.withType(BYTES).build(), value == null ? null : (byte[]) value);
                    break;
                case Types.BIGINT:
                    builder.withLong(entryBuilder.withType(LONG).build(), (Long) value);
                    break;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                default:
                    builder.withString(entryBuilder.withType(STRING).build(), value == null ? null : String.valueOf(value));
                    break;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void emptyTable(final Record.Builder builder, final ResultSetMetaData metaData, final int columnIndex)
    {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final String integerType = Integer.class.getName();
            final int sqlType = metaData.getColumnType(columnIndex);
            final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != columnNoNulls);


            switch (sqlType) {
                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                    if (javaType.equals(integerType)) {
                        builder.withInt(entryBuilder.withType(INT).build(), 0);
                    } else {
                        builder.withLong(entryBuilder.withType(LONG).build(), 0L);
                    }
                    break;
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.NUMERIC:

                    builder.withFloat(entryBuilder.withType(FLOAT).build(), 0.0F);
                    break;
                case Types.DOUBLE:
                    builder.withDouble(entryBuilder.withType(DOUBLE).build(), 0.0);
                    break;
                case Types.BOOLEAN:
                    builder.withBoolean(entryBuilder.withType(BOOLEAN).build(), false);
                    break;
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    builder.withDateTime(entryBuilder.withType(DATETIME).build(), new java.util.Date());
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    builder.withBytes(entryBuilder.withType(BYTES).build(), "NULL".getBytes());
                    break;
                case Types.BIGINT:
                    builder.withLong(entryBuilder.withType(LONG).build(), 0L);
                    break;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                default:
                    builder.withString(entryBuilder.withType(STRING).build(), "NULL");
                    break;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}