package com.examplesql.talend.components.output.statement.operations;

import com.examplesql.talend.components.output.OutputConfiguration;
import com.examplesql.talend.components.output.RecordToSQLTypeConverter;
import com.examplesql.talend.components.output.Reject;
import com.examplesql.talend.components.service.I18nMessage;
import com.examplesql.talend.components.service.ExampleSQLComponentService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class UpsertDefault extends QueryManagerImpl {

    private final Insert insert;

    private final Update update;

    private final List<String> keys;

    private Map<Integer, Schema.Entry> queryParams;

    public UpsertDefault(final OutputConfiguration configuration, final I18nMessage i18n) {
        super(i18n, configuration);
        queryParams = new HashMap<>();
        this.keys = new ArrayList<>(ofNullable(configuration.getKeys()).orElse(emptyList()));
        if (this.keys.isEmpty()) {
            throw new IllegalArgumentException(i18n.errorNoKeyForUpdateQuery());
        }
        insert = new Insert(configuration, i18n);
        update = new Update(configuration, i18n);
    }

    public Insert getInsert() { return this.insert; }

    public Update getUpdate() { return this.update; }

    public List<String> getKeys() { return this.keys; }

    @Override
    public String buildQuery(final List<Record> records) {
        //this.queryParams = new HashMap<>();
        final AtomicInteger index = new AtomicInteger(0);
        final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                .collect(toList());

        return "SELECT COUNT(*) AS RECORD_EXIST FROM " + identifier(getConfiguration().getDataset().getTableName())
                + " WHERE "
                + getConfiguration().getKeys().stream()
                .peek(key -> queryParams.put(index.incrementAndGet(),
                        entries.stream().filter(e -> e.getName().equals(key)).findFirst()
                                .orElseThrow(() -> new IllegalStateException(getI18n().errorNoFieldForQueryParam(key)))))
                .map(c -> identifier(c)).map(c -> c + " = ?").collect(joining(" AND "));
    }

    @Override
    public boolean validateQueryParam(final Record record) {
        final Set<Schema.Entry> entries = new HashSet<>(record.getSchema().getEntries());
        return keys.stream().allMatch(k -> entries.stream().anyMatch(entry -> entry.getName().equals(k)))
                && entries.stream().filter(entry -> keys.contains(entry.getName())).filter(entry -> !entry.isNullable())
                .map(entry -> valueOf(record, entry)).allMatch(Optional::isPresent);
    }

    @Override
    public Map<Integer, Schema.Entry> getQueryParams() {
       return Collections.unmodifiableMap(queryParams);
        // return queryParams;
    }

    @Override
    public List<Reject> execute(final List<Record> records, final ExampleSQLComponentService.DataSource dataSource) throws SQLException {
        if (records.isEmpty()) {
            return emptyList();
        }
        final List<Record> needUpdate = new ArrayList<>();
        final List<Record> needInsert = new ArrayList<>();
        final String query = buildQuery(records);
        final List<Reject> discards = new ArrayList<>();
        try (final Connection connection = dataSource.getConnection()) {
            try (final PreparedStatement statement = connection.prepareStatement(query)) {
                for (final Record record : records) {
                    statement.clearParameters();
                    if (!validateQueryParam(record)) {
                        discards.add(new Reject("missing required query param in this record", record));
                        continue;
                    }
                    for (final Map.Entry<Integer, Schema.Entry> entry : getQueryParams().entrySet()) {
                        RecordToSQLTypeConverter.valueOf(entry.getValue().getType().name()).setValue(statement, entry.getKey(),
                                entry.getValue(), record);
                    }
                    try (final ResultSet result = statement.executeQuery()) {
                        if (result.next() && result.getInt("RECORD_EXIST") > 0) {
                            needUpdate.add(record);
                        } else {
                            needInsert.add(record);
                        }
                    }
                }
                connection.commit();
            } catch (final SQLException e) {
                connection.rollback();
                throw e;
            }
        }

        // fixme handle the update and insert in // need a pool of 2 !
        if (!needInsert.isEmpty()) {
            insert.buildQuery(needInsert);
            discards.addAll(insert.execute(needInsert, dataSource));
        }
        if (!needUpdate.isEmpty()) {
            update.buildQuery(needUpdate);
            discards.addAll(update.execute(needUpdate, dataSource));
        }

        return discards;
    }

    @Override
    public void load(final Connection connection) throws SQLException {}
}