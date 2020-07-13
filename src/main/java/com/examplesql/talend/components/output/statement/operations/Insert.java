package com.examplesql.talend.components.output.statement.operations;

import com.examplesql.talend.components.output.OutputConfiguration;
import com.examplesql.talend.components.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class Insert extends QueryManagerImpl {

    private Map<Integer, Schema.Entry> namedParams;

    private final Map<String, String> queries = new HashMap<>();

    public Insert(final OutputConfiguration configuration, final I18nMessage i18n) {
        super(i18n, configuration);
        namedParams = new HashMap<>();
    }

    @Override
    public String buildQuery(final List<Record> records) {
        final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                .collect(toList());
        return queries.computeIfAbsent(entries.stream().map(Schema.Entry::getName).collect(joining("::")), key -> {
            final AtomicInteger index = new AtomicInteger(0);
           // namedParams = new HashMap<>();
            entries.forEach(name -> namedParams.put(index.incrementAndGet(), name));
            final List<Map.Entry<Integer, Schema.Entry>> params = namedParams.entrySet().stream()
                    .sorted(comparing(Map.Entry::getKey)).collect(toList());
            final StringBuilder query = new StringBuilder("INSERT INTO ")
                    .append(identifier(getConfiguration().getDataset().getTableName()));
            query.append(params.stream().map(e -> e.getValue().getName()).map(name -> identifier(name))
                    .collect(joining(",", "(", ")")));
            query.append(" VALUES");
            query.append(params.stream().map(e -> "?").collect((joining(",", "(", ")"))));
            return query.toString();
        });
    }

    @Override
    public boolean validateQueryParam(final Record record) {
        return namedParams.values().stream().filter(e -> !e.isNullable()).map(e -> valueOf(record, e))
                .allMatch(Optional::isPresent);
    }

    @Override
    public Map<Integer, Schema.Entry> getQueryParams() {
        //return namedParams;
        return Collections.unmodifiableMap(namedParams);
    }

    @Override
    public void load(final Connection connection) throws SQLException {}
}