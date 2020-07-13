package com.examplesql.talend.components.output.statement.operations;

import com.examplesql.talend.components.output.OutputConfiguration;
import com.examplesql.talend.components.output.RecordToSQLTypeConverter;
import com.examplesql.talend.components.output.Reject;
import com.examplesql.talend.components.output.statement.QueryManager;
import com.examplesql.talend.components.service.I18nMessage;
import com.examplesql.talend.components.service.ExampleSQLComponentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.*;
import java.util.*;

import static java.util.Collections.emptyList;
import static java.util.Optional.*;
import static java.util.stream.Collectors.toList;

public abstract class QueryManagerImpl implements QueryManager {

    private static final transient Logger LOG = LoggerFactory.getLogger(QueryManagerImpl.class);

    private final OutputConfiguration configuration;

    private final I18nMessage i18n;

    private final Integer maxRetry = 10;

    private Integer retryCount = 0;

    abstract protected String buildQuery(List<Record> records);

    abstract protected Map<Integer, Schema.Entry> getQueryParams();

    abstract protected boolean validateQueryParam(Record record);

    abstract public void load(final Connection connection) throws SQLException;

    public QueryManagerImpl(final I18nMessage i18n, final OutputConfiguration configuration)
    {
        this.i18n = i18n;
        this.configuration = configuration;
    }

    public OutputConfiguration getConfiguration() { return this.configuration; }

    public void truncate(String tableName, final ExampleSQLComponentService.DataSource dataSource) throws SQLException {
        try (final Connection connection = dataSource.getConnection()) {
            connection.createStatement().executeQuery("truncate table " + tableName);
        }
    }

    public I18nMessage getI18n() { return this.i18n; }
    @Override
    public List<Reject> execute(final List<Record> records, final ExampleSQLComponentService.DataSource dataSource) throws SQLException {
        if (records.isEmpty()) {
            return emptyList();
        }
        try (final Connection connection = dataSource.getConnection()) {
            return processRecords(records, connection, buildQuery(records));
        }
    }

    private List<Reject> processRecords(final List<Record> records, final Connection connection, final String query)
            throws SQLException {
        List<Reject> rejects;
        do {
            rejects = new ArrayList<>();
            try (final PreparedStatement statement = connection.prepareStatement(query)) {
                final Map<Integer, Integer> batchOrder = new HashMap<>();
                int recordIndex = -1;
                int batchNumber = -1;
                for (final Record record : records) {
                    recordIndex++;
                    statement.clearParameters();
                    if (!validateQueryParam(record)) {
                        rejects.add(new Reject("missing required query param in this record", record));
                        continue;
                    }
                    for (final Map.Entry<Integer, Schema.Entry> entry : getQueryParams().entrySet()) {
                        RecordToSQLTypeConverter.valueOf(entry.getValue().getType().name()).setValue(statement, entry.getKey(),
                                entry.getValue(), record);
                    }
                    statement.addBatch();
                    batchNumber++;
                    batchOrder.put(batchNumber, recordIndex);
                }

                try {
                    statement.executeBatch();
                    connection.commit();
                    break;
                } catch (final SQLException e) {
                    if (!connection.getAutoCommit()) {
                        connection.rollback();
                    }
                    if (!retry(e) || retryCount > maxRetry) {
                        rejects.addAll(handleRejects(records, batchOrder, e));
                        break;
                    }
                    retryCount++;
                    LOG.warn("Deadlock detected. retrying for the " + retryCount + " time", e);
                    try {
                        Thread.sleep((long) Math.exp(retryCount) * 2000);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } while (true);

        return rejects;
    }

    private boolean retry(final SQLException e) {
        return "40001".equals(ofNullable(e.getNextException()).orElse(e).getSQLState());
    }

    private List<Reject> handleRejects(final List<Record> records, Map<Integer, Integer> batchOrder, final SQLException e)
            throws SQLException {
        if (!(e instanceof BatchUpdateException)) {
            throw e;
        }
        final List<Reject> discards = new ArrayList<>();
        final int[] result = ((BatchUpdateException) e).getUpdateCounts();
        SQLException error = e;
        if (result.length == records.size()) {
            for (int i = 0; i < result.length; i++) {
                if (result[i] == Statement.EXECUTE_FAILED) {
                    error = ofNullable(error.getNextException()).orElse(error);
                    discards.add(new Reject(error.getMessage(), error.getSQLState(), error.getErrorCode(),
                            records.get(batchOrder.get(i))));
                }
            }
        } else {
            int failurePoint = result.length;
            error = ofNullable(error.getNextException()).orElse(error);
            discards.add(new Reject(error.getMessage(), error.getSQLState(), error.getErrorCode(),
                    records.get(batchOrder.get(failurePoint))));
            // todo we may retry for this sub list
            discards.addAll(records.subList(batchOrder.get(failurePoint) + 1, records.size()).stream()
                    .map(r -> new Reject("rejected due to error in previous elements error in this transaction", r))
                    .collect(toList()));
        }

        return discards;
    }

    public String namespace(final Connection connection) throws SQLException {
        return (connection.getCatalog() != null && !connection.getCatalog().isEmpty()
                ? identifier(connection.getCatalog()) + "."
                : "")
                + (connection.getSchema() != null && !connection.getSchema().isEmpty()
                ? identifier(connection.getSchema())
                : "");
    }

    public static Optional<Object> valueOf(final Record record, final Schema.Entry entry) {
        switch (entry.getType()) {
            case INT:
                return record.getOptionalInt(entry.getName()).isPresent() ? of(record.getOptionalInt(entry.getName()).getAsInt())
                        : empty();
            case LONG:
                return record.getOptionalLong(entry.getName()).isPresent() ? of(record.getOptionalLong(entry.getName()).getAsLong())
                        : empty();
            case FLOAT:
                return record.getOptionalFloat(entry.getName()).isPresent()
                        ? of(record.getOptionalFloat(entry.getName()).getAsDouble())
                        : empty();
            case DOUBLE:
                return record.getOptionalDouble(entry.getName()).isPresent()
                        ? of(record.getOptionalDouble(entry.getName()).getAsDouble())
                        : empty();
            case BOOLEAN:
                return record.getOptionalBoolean(entry.getName()).isPresent() ? of(record.getOptionalBoolean(entry.getName()).get())
                        : empty();
            case BYTES:
                return record.getOptionalBytes(entry.getName()).isPresent() ? of(record.getOptionalBytes(entry.getName()).get())
                        : empty();
            case DATETIME:
                return record.getOptionalDateTime(entry.getName()).isPresent() ? of(record.getOptionalDateTime(entry.getName()).get())
                        : empty();
            case STRING:
                return record.getOptionalString(entry.getName()).isPresent() ? of(record.getOptionalString(entry.getName()).get())
                        : empty();
            case RECORD:
                return record.getOptionalRecord(entry.getName()).isPresent() ? of(record.getOptionalRecord(entry.getName()).get())
                        : empty();
            case ARRAY:
            default:
                return empty();
        }
    }

    public String identifier(final String name) {
        return name == null || name.isEmpty() ? name : delimiterToken() + name + delimiterToken();
    }

    protected String delimiterToken() {
        return "`";
    }
}
