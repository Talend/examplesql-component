package com.examplesql.talend.components.output.statement.operations;

import com.examplesql.talend.components.output.OutputConfiguration;
import com.examplesql.talend.components.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class Update extends QueryManagerImpl {

    private final List<String> keys;

    private final List<String> ignoreColumns;

    private Map<Integer, Schema.Entry> queryParams;

    public Update(final OutputConfiguration configuration, final I18nMessage i18n) {
        super(i18n, configuration);
        queryParams = new HashMap<>();
        this.keys = new ArrayList<>(ofNullable(configuration.getKeys()).orElse(emptyList()));
        if (this.keys.isEmpty()) {
            throw new IllegalArgumentException(i18n.errorNoKeyForUpdateQuery());
        }
        this.ignoreColumns = new ArrayList<>(ofNullable(configuration.getIgnoreUpdate()).orElse(emptyList()));
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
        //return queryParams;
    }

    @Override
    public String buildQuery(final List<Record> records) {
        //this.queryParams = new HashMap<>();
        final AtomicInteger index = new AtomicInteger(0);
        final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                .collect(toList());
        final String query = "UPDATE " + identifier(getConfiguration().getDataset().getTableName()) + " SET "
                + entries.stream().filter(e -> !ignoreColumns.contains(e.getName()) && !keys.contains(e.getName()))
                .peek(e -> queryParams.put(index.incrementAndGet(), e)).map(c -> identifier(c.getName()))
                .map(c -> c + " = ?").collect(joining(","))
                + " WHERE " + keys.stream().map(c -> identifier(c)).map(c -> c + " = ?").collect(joining(" AND "));

        keys.stream()
                .map(key -> entries.stream().filter(e -> key.equals(e.getName())).findFirst()
                        .orElseThrow(() -> new IllegalStateException(getI18n().errorNoFieldForQueryParam(key))))
                .forEach(entry -> queryParams.put(index.incrementAndGet(), entry));
        return query;
    }



    @Override
    public void load(final Connection connection) throws SQLException {}
}
