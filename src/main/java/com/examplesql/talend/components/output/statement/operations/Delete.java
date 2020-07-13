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

public class Delete extends QueryManagerImpl {

    private final List<String> keys;

    private Map<Integer, Schema.Entry> queryParams;

    private final String query;

    private boolean namedParamsResolved;

    public Delete(final OutputConfiguration configuration, final I18nMessage i18n) {
        super(i18n, configuration);
        queryParams = new HashMap<>();
        this.keys = new ArrayList<>(ofNullable(configuration.getKeys()).orElse(emptyList()));
        if (this.keys.isEmpty()) {
            throw new IllegalArgumentException(getI18n().errorNoKeyForDeleteQuery());
        }
        this.query = "DELETE FROM " + identifier(configuration.getDataset().getTableName()) + " WHERE "
                + keys.stream().map(c -> c + " = ?").collect(joining(" AND "));
    }

    @Override
    public String buildQuery(final List<Record> records) {
        if (!namedParamsResolved) {
            //queryParams = new HashMap<>();
            final AtomicInteger index = new AtomicInteger(0);
            final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                    .collect(toList());
            keys.stream().map(key -> entries.stream().filter(e -> key.equals(e.getName())).findFirst())
                    .filter(Optional::isPresent).map(Optional::get)
                    .forEach(entry -> queryParams.put(index.incrementAndGet(), entry));
            /* can't handle this group without all the named params */
            if (queryParams.size() != keys.size()) {
                final String missingParams = keys.stream()
                        .filter(key -> queryParams.values().stream().noneMatch(e -> e.getName().equals(key)))
                        .collect(joining(","));
                throw new IllegalStateException(new IllegalStateException(getI18n().errorNoFieldForQueryParam(missingParams)));
            }

            namedParamsResolved = true;
        }
        return query;
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
    public void load(final Connection connection) throws SQLException {}
}
