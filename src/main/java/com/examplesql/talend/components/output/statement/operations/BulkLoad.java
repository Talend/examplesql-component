package com.examplesql.talend.components.output.statement.operations;

import com.examplesql.talend.components.output.OutputConfiguration;
import com.examplesql.talend.components.output.RecordToBulkLoadConverter;
import com.examplesql.talend.components.output.Reject;
import com.examplesql.talend.components.service.I18nMessage;
import com.examplesql.talend.components.service.ExampleSQLComponentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class BulkLoad extends QueryManagerImpl {
    private static final transient Logger LOG = LoggerFactory.getLogger(BulkLoad.class);
    private String filePath = null;
    private Map<Integer, Schema.Entry> namedParams;



    public BulkLoad(final OutputConfiguration configuration, final I18nMessage i18n) {
        super(i18n, configuration);
        namedParams = new HashMap<>();

    }

    @Override
    protected String buildQuery(List<Record> records) {
        final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                .collect(toList());
        //namedParams = new HashMap<>();
        final AtomicInteger index = new AtomicInteger(0);
        entries.forEach(name -> namedParams.put(index.incrementAndGet(), name));

        return null;
    }

    @Override
    protected Map<Integer, Schema.Entry> getQueryParams() {
        return Collections.unmodifiableMap(namedParams);
        //return namedParams;
    }

    @Override
    protected boolean validateQueryParam(final Record record) {
        return namedParams.values().stream().filter(e -> !e.isNullable()).map(e -> valueOf(record, e))
                .allMatch(Optional::isPresent);
    }

    @Override
    public List<Reject> execute(final List<Record> records, final ExampleSQLComponentService.DataSource dataSource) throws SQLException {
        if (records.isEmpty()) {
            return emptyList();
        }
        try (final Connection connection = dataSource.getConnection()) {
            buildQuery(records);
            return processRecords(records, connection);
        }
    }

    private List<Reject> processRecords(final List<Record> records, final Connection connection)
            throws SQLException {
        List<Reject> rejects = new ArrayList<Reject>();
        BulkLoadWriter writer = BulkLoadWriter.getInstance(getConfiguration().getDataset().getTableName());
        try {
            if (filePath == null) filePath = writer.getDirPath();
        } catch(Exception e) {this.filePath = ".";}

        StringBuilder buffer = new StringBuilder();
        List<String> lines = new ArrayList<String>();


        for (final Record record : records) {
            buffer.setLength(0);
            if (!validateQueryParam(record)) {
                rejects.add(new Reject("missing required query param in this record", record));
                continue;
            }
            for (final Map.Entry<Integer, Schema.Entry> entry : getQueryParams().entrySet()) {
                RecordToBulkLoadConverter.valueOf(entry.getValue().getType().name()).setValue(lines, entry.getKey(),
                        entry.getValue(), record);
            }
            buffer.append(lines.stream().collect(Collectors.joining(","))+"\n");
            writer.write(buffer.toString());
           
            lines.clear();
        }

        writer.close();
        return rejects;
    }

    @Override
    public void load(final Connection connection) throws SQLException {
        long start = new Date().getTime();
        
            File dir = new File(this.filePath);
            File[] files = dir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                return name.startsWith(getConfiguration().getDataset().getTableName());
                }
            });
            for (File file: files)
            {
                
                int rows = connection.createStatement().executeUpdate("LOAD DATA LOCAL INFILE '"+file.getAbsolutePath()+"' INTO TABLE " + getConfiguration().getDataset().getTableName() + " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n'");
                LOG.debug("Rows Processed " + rows + "  for file " + file.getAbsolutePath());
                connection.commit();
                file.delete();
            }
        System.out.println("Load Data Execution Time: " + (new Date().getTime() - start) + " milliseconds");
    }
}